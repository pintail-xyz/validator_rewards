import psycopg2

# open/restart connection to chaind database
try:
    cursor.close()
    connection.close()
except:
    pass

connection = psycopg2.connect(user="chain", host="127.0.0.1", database="chain", password="medalla")
cursor = connection.cursor()

# basic info about the dataset
cursor.execute("SELECT MAX(f_slot) FROM t_blocks")
latest_slot = cursor.fetchone()[0]
n_slots = latest_slot + 1
n_epochs = (n_slots - (n_slots % 32)) // 32
print(f"latest slot: {latest_slot}, latest complete epoch: {n_epochs - 1}")

cursor.execute("SELECT f_slot, f_root FROM t_blocks ORDER BY f_slot DESC LIMIT 1")
latest_block = cursor.fetchone()
slot, root = latest_block[0], latest_block[1].hex()
print(f"latest block root: {root}")

top = "SELECT f_value FROM t_metadata WHERE f_key = '"
tail = "';"

cursor.execute(top + 'beaconcommittees.standard' + tail)
result = cursor.fetchone()[0]
print("beacon committees\n    latest epoch: " + str(result['latest_epoch']))
if 'missed_epochs' in result:
    n_missed = len(result['missed_epochs'])
    if n_missed == 1:
        print('    missing epoch ' + str(result['missed_epochs'][0]))
    else:
        print('    ' + str(n_missed) + ' missed epochs')

cursor.execute(top + 'blocks.standard' + tail)
result = cursor.fetchone()[0]
print("blocks\n    latest slot: " + str(result['latest_slot']))
if 'missed_slots' in result:
    n_missed = len(result['missed_slots'])
    if n_missed == 1:
        print('    missing slot ' + str(result['missed_epochs'][0]))
    else:
        print('    ' + str(n_missed) + ' missed slots')


cursor.execute(top + 'proposerduties.standard' + tail)
result = cursor.fetchone()[0]
print("proposer duties\n    latest epoch: " + str(result['latest_epoch']))
if 'missed_epochs' in result:
    n_missed = len(result['missed_epochs'])
    if n_missed == 1:
        print('    missing epoch' + str(result['missed_epochs'][0]))
    else:
        print('    ' + str(n_missed) + ' missed epochs')

cursor.execute(top + 'validators.standard' + tail)
result = cursor.fetchone()[0]
print("validators\n    latest epoch: " + str(result['latest_epoch']))
if 'missed_epochs' in result:
    n_missed = len(result['missed_epochs'])
    if n_missed == 1:
        print('    missing epoch ' + str(result['missed_epochs'][0]))
    else:
        print('    ' + str(n_missed) + ' missed epochs')
