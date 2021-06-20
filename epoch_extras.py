import time

import psycopg2

FAR_FUTURE_EPOCH = 2**64 - 1 # as defined in spec

connection = psycopg2.connect(user="chain", host="127.0.0.1", database="chain", password="medalla")
cursor = connection.cursor()

def print_progress(start_time, current_item, n_items):
    seconds = time.time() - start_time
    elapsed = time.strftime("%H:%M:%S",time.gmtime(seconds))
    left = time.strftime("%H:%M:%S",time.gmtime(seconds * ((n_items) / (current_item+1)-1)))
    perc = 100*(current_item+1)/n_items
    print(f"iteration {current_item} of {n_items} ({perc:.2f}%) / {elapsed} elapsed / {left} left", end='\r')

# get validator and epoch summary data

cursor.execute("SELECT f_activation_eligibility_epoch, f_activation_epoch, f_exit_epoch, "
               "f_slashed, f_public_key FROM t_validators ORDER BY f_index")
result = cursor.fetchall()
validators = [{
    "index"                       : i,
    "activation_epoch"            : r[1],
    "exit_epoch"                  : r[2],
    "slashed"                     : r[3],
    "slashed_slot"                : None,
    "slasher"                     : False,
    "pubkey"                      : r[4].hex()
} for i, r in enumerate(result)]

pubkey_lookup = {r[4].hex(): validators[i] for i, r in enumerate(result)}

for v in validators:
    if v["activation_epoch"] is None:
        v["activation_epoch"] = FAR_FUTURE_EPOCH
    if v["exit_epoch"] is None:
        v["exit_epoch"] = FAR_FUTURE_EPOCH

cursor.execute("SELECT * FROM t_epoch_summaries WHERE f_epoch < 32002 ORDER BY f_epoch")
result = cursor.fetchall()
epoch_summaries = [{
    "epoch"                           : r[0],
    "active_validators"               : r[3],
    "active_real_balance"             : r[4],
    "active_balance"                  : r[5],
    "attesting_validators"            : r[6],
    "attesting_balance"               : r[7],
    "target_correct_validators"       : r[8],
    "target_correct_balance"          : r[9],
    "head_correct_validators"         : r[10],
    "head_correct_balance"            : r[11],
} for r in result]

# check for any instances of validator deposits made to already active validators

cursor.execute("SELECT f_inclusion_slot, f_validator_pubkey, f_amount FROM t_deposits")
deposits = cursor.fetchall()

repeat_deposit_count = 0
repeat_deposit_epochs = {}
for deposit in deposits:
    slot, tmp, amount = deposit
    pubkey = tmp.hex()
    if pubkey not in pubkey_lookup:
        continue
    else:
        validator = pubkey_lookup[deposit[1].hex()]
    if slot // 32 > validator["activation_epoch"] and slot // 32 < validator["exit_epoch"]:
        repeat_deposit_count += 1
        epoch = (slot - 1) // 32 + 1 # NB chaind balances are based on the state *after* processing slot 0
        #print(f"validator {validator['index']} made duplicate deposit of {amount} in epoch {epoch}")
        cursor.execute(f"SELECT f_epoch, f_balance FROM t_validator_balances "
                       f"WHERE f_epoch BETWEEN {epoch-1} AND {epoch+1} "
                       f"AND f_validator_index = {validator['index']}")
        balances = cursor.fetchall()
        #for row in balances:
        #    print(f"epoch: {row[0]}, balance: {row[1]}")

        if epoch in repeat_deposit_epochs:
            repeat_deposit_epochs[epoch] += deposit[2]
        else:
            repeat_deposit_epochs[epoch] = deposit[2]

print(f"{repeat_deposit_count} repeat deposits found")

# identify slashers

cursor.execute("SELECT f_inclusion_slot FROM t_proposer_slashings")
proposer_slashings = [s[0] for s in cursor.fetchall()]

cursor.execute("SELECT f_inclusion_slot FROM t_attester_slashings")
slashings = proposer_slashings + [s[0] for s in cursor.fetchall()]

n_slashers = 0
for s in slashings:
    cursor.execute(f"SELECT f_validator_index FROM t_proposer_duties WHERE f_slot = {s}")
    slasher = cursor.fetchone()[0]
    validators[slasher]["slasher"] = True
    n_slashers += 1

print(f"identified {n_slashers} slashers")

# calculate aggregate net rewards (from the change in the balances of active validators)

cursor.execute("SELECT f_balance FROM t_validator_balances WHERE f_epoch = 1 ORDER BY f_validator_index")
prior_balances = cursor.fetchall()

start_time = time.time()
for e, s in enumerate(epoch_summaries):
    if e+2 >= len(epoch_summaries):
        break
    cursor.execute(f"SELECT f_effective_balance FROM t_validator_balances "
                   f"WHERE f_epoch = {e} ORDER BY f_validator_index")
    effective_balances = [b[0] for b in cursor.fetchall()]
    s["active_balance_nonslashed"] = 0
    s["aggregate_net_reward"] = 0
    s["aggregate_net_reward_nonslashed"] = 0
    if e+2 in repeat_deposit_epochs:
        s["aggregate_net_reward"] -= repeat_deposit_epochs[e+2]
        s["aggregate_net_reward_nonslashed"] -= repeat_deposit_epochs[e+2]

    cursor.execute(f"SELECT f_balance FROM t_validator_balances "
                   f"WHERE f_epoch = {e+2} ORDER BY f_validator_index")
    new_balances = cursor.fetchall()
    for validator_index, balance in enumerate(new_balances):
        validator = validators[validator_index]
        if validator["activation_epoch"] <= e and (validator["exit_epoch"] > e):
            net_reward = balance[0] - prior_balances[validator_index][0]
            s["aggregate_net_reward"] += net_reward
            if not validator["slashed"] and not validator["slasher"]:
                s["aggregate_net_reward_nonslashed"] += net_reward
                s["active_balance_nonslashed"] += effective_balances[validator_index]

    prior_balances = new_balances
    print_progress(start_time, e, len(epoch_summaries) - 2)

# save results in chaind database

print()
print("writing to database")

cursor.execute("TRUNCATE t_epoch_extras")
connection.commit()
for s in epoch_summaries:
    if "aggregate_net_reward" in s:
        e = s["epoch"]
        anr = s["aggregate_net_reward"]
        anrns = s["aggregate_net_reward_nonslashed"]
        abns = s["active_balance_nonslashed"]
        cursor.execute(f"INSERT INTO t_epoch_extras VALUES ({e}, {anr}, {anrns}, {abns})")

print("finished db insertions")
connection.commit()
cursor.close()
connection.close()
print("done")
