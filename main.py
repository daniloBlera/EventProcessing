# *-* coding: utf-8 *-*
from datetime import datetime

import pika
import pymysql


posts = []
comment_post_table = []
unified_structures = []

top3 = None

current_sim_time = datetime(2000, 1, 1, 0, 0)
current_timestamp = None
last_update_timestamp = None
current_top3 = None
free_to_send = True

exchange_name = "amq.topic"
queue_name = "SOCIAL_NETWORK_EVENTS"
spark_queue = "SPARK_POST_STRUCTURES"
parameters = pika.ConnectionParameters(host='localhost', port=5672)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.exchange_declare(exchange=exchange_name, type='topic', durable=True)

# channel.queue_declare(queue=queue_name)
# channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key="#")

def consume_handler(ch, method, properties, body):
    """
    Função de callback que implementa o consumo de mensagens do serviço de
    filas.
    """
    global free_to_send

    if method.routing_key == "posts":
        print("RECIEVED EVENT: {}".format(method.routing_key))
        insert_post(body)
    elif method.routing_key == "comments":
        print("RECIEVED EVENT: {}".format(method.routing_key))
        insert_comment(body)
    elif method.routing_key == "SPARK_PROCESSING_RESPONSE":
        print("RECIEVED EVENT: {}".format(method.routing_key))
        free_to_send = True

        if body == 'None':
            return None

        print("POSTS' ID-SCORE PAIRS RECIEVED FROM SPARK:")
        scores = body.split('>>')
        for s in scores:
            print(s)
        print("\n")

        update_post_structures(body)

    if free_to_send:
        send_structures_for_processing()

def insert_post(post):
    pair = (post.split('|')[1], [post.strip('\n')])
    posts.append(pair)
    unified_structures.append(pair)
    update_current_timestamp(post.split('|')[0])


def insert_comment(comment):
    root_id = get_root_post_id_from(comment)

    if root_id:
        for i in range(0, len(unified_structures)):
            if unified_structures[i][0] == root_id:
                unified_structures[i][1].append(comment)

        comment_post_table.append((comment.split('|')[1], root_id))

    update_current_timestamp(comment.split('|')[0])


def get_root_post_id_from(comment):
    parent_ids = comment.split('|')[5:7]
    root_id = None

    if parent_ids[1] != '':
        root_id = parent_ids[1]
    else:
        for c in comment_post_table:
            if parent_ids[0] == c[0]:
                root_id = c[1]
                break

    return root_id

def print_unified_structures_contents():
    for struct in unified_structures:
        print("ID: {}".format(struct[0]))
        for i in range(len(struct[1])):
            if i == 0:
                print("POST: {}".format(struct[1][i]))
            else:
                print("COMM: {}".format(struct[1][i]))

        print("\n")

def send_structures_for_processing():
    global free_to_send
    global last_update_timestamp

    free_to_send = False
    last_update_timestamp = current_timestamp
    sep = ">>"

    for struct in unified_structures:

        channel.basic_publish(
            exchange=exchange_name,
            routing_key=spark_queue,
            body=str(current_timestamp + sep + sep.join(struct[1]))
        )

def update_current_sim_time(timestamp):
    ts_fmt = "%Y-%m-%dT%H:%M:%S.%f+0000"

    current_sim_time = datetime.strptime(timestamp, ts_fmt)

def update_current_timestamp(timestamp):
    global current_timestamp
    current_timestamp = timestamp

def get_ids_to_delete_from(message):
    scores = message.split(">>")

    inactive_ids = []

    for s in scores:
        if int(s.split(',')[1]) <= 0:
            inactive_ids.append(s.split(',')[0])

    return inactive_ids

def get_top3_from(message):
    scores = message.split(">>")

    id_score_pairs = []
    for line in scores:
        if line.split(',')[1] > 0:
            id_score_pairs.append(line.split(','))

    id_score_pairs.sort(reverse=True)
    return id_score_pairs[0:3]

def get_top3_ids_from(message):
    scores = message.split(">>")

    post_ids = []
    for s in scores:
        id_score = s.split(',')

        if int(id_score[1]) > 0:
            post_ids.append(id_score[0])

    post_ids.sort(reverse=True)

    return post_ids[0:3]

def is_current_top3_equals_to(new_top3):
    new_ids = []
    if not new_top3:
        new_ids = None
    else:
        for pair in new_top3:
            new_ids.append(pair[0])

    top3_ids = []
    if not top3:
        top3_ids = None
    else:
        for pair in top3:
            top3_ids.append(pair[0])

    return new_ids == top3_ids

def remove_inactive_posts(message):
    global unified_structures
    posts_to_remove = get_ids_to_delete_from(message)

    if posts_to_remove:
        print("INACTIVE POST IDs")
        for e in posts_to_remove:
            print(e)
        print("\n")

    new_unified_structs = []

    for i in range(0, len(unified_structures)):
        if unified_structures[i][0] not in posts_to_remove:
            new_unified_structs.append(unified_structures[i])

    unified_structures = new_unified_structs

def update_post_structures(message):
    remove_inactive_posts(message)
    update_top3_posts(message)

def get_connection_instance():
    connection = pymysql.connect(
        host='localhost', user='root', password='this is my root',
        db='RsiPsd', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    return connection

def send_mysql_top3_update():
    lines = get_mysql_update_params()

    connection = get_connection_instance()

    try:
        with connection.cursor() as cursor:
            # Create a new record
            # sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
            # cursor.execute(sql, ('webmaster@python.org', 'very-secret'))

            query = ("INSERT INTO `RsiPsd`.`top3Posts` (`timeChanged`, " +
                     "`idPost1`, `userPost1`, `postScore1`, `numComPost1`, " +
                     "`idPost2`, `userPost2`, `postScore2`, `numComPost2`, " +
                     "`idPost3`, `userPost3`, `postScore3`, `numComPost3`) " +
                     "VALUE (" +
                     "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);")

            cursor.execute(query, lines)
        connection.commit()
    finally:
        connection.close()

def get_mysql_update_params():
    ts = last_update_timestamp.replace('T', ' ')

    line1 = get_params_from(0)
    print("\nLINE1: {}".format(line1))

    line2 = get_params_from(1)
    print("LINE2: {}".format(line2))

    line3 = get_params_from(2)
    print("LINE3: {}\n".format(line3))

    return (ts + ',' + line1 + ',' + line2 + ',' + line3).split(',')

def get_params_from(index):
    try:
        post_id = top3[index][0]
        score = top3[index][1]
        structures = None

        for s in unified_structures:
            if s[0] == post_id:
                structures = s[1]
                break

        op_name = structures[0].split('|')[4].strip('\n')
        op_id = structures[0].split('|')[2]

        commenters_count = 0
        for event in structures:
            if event.split('|')[2] != op_id:
                commenters_count += 1

        return post_id + ',' + op_name + ',' + score + ',' + str(commenters_count)
    except IndexError, AttributeError:
        return "-,-,-,-"

def update_top3_posts(message):
    global top3

    new_top3 = get_top3_from(message)

    print("RECIEVED TOP3 ID-Score PAIRS")
    for e in new_top3:
        print(e)
    print('\n')

    print("CURRENT TOP3")
    if not top3: print(top3)
    else:
        for e in top3:
            print(e)
    print('\n')

    if is_current_top3_equals_to(new_top3):
        return

    top3 = new_top3

    print("--TOP3 HAS CHANGED--")
    for e in new_top3:
        print(e)
    print("\n")

    print("THIS HAPPENED AT {}\n".format(last_update_timestamp))
    send_mysql_top3_update()


if __name__ == "__main__":
    print("Leitura de eventos iniciada...")
    try:
        channel.basic_consume(consume_handler, queue=queue_name, no_ack=True)
        channel.start_consuming()
    except KeyboardInterrupt:
        print "\nProcessamento interrompido"
