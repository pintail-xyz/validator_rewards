import sys
import math

from chaind_extras import ChainDB

EB_INCREMENT = int(1e9)

if __name__ == '__main__':

    reset = True if '--reset' in sys.argv or '-r' in sys.argv else False
    chaind = ChainDB(reset=reset)

    latest_epoch = chaind.get_latest_extras_epoch()
    if latest_epoch is None:
        e = 0
    else:
        e = latest_epoch + 1

    summary_e1 = chaind.get_epoch_summary_balances(e)
    filled_slots = [0] * 64
    filled_slots[32:] = chaind.get_filled_slots(e)

    base_reward = [0] * 32
    i_reward = base_reward.copy()
    t_reward = base_reward.copy()
    h_reward = base_reward.copy()
    max_d_reward = base_reward.copy()
    max_reward = base_reward.copy()

    latest_epoch = chaind.get_latest_summary_epoch()
    while e < latest_epoch - 1: # iterate through available epochs
        summary_e = summary_e1
        summary_e1 = chaind.get_epoch_summary_balances(e+1)
        filled_slots[:32] = filled_slots[32:]
        filled_slots[32:] = chaind.get_filled_slots(e+1)

        # calculate attestation rewards available for this epoch

        for eb in range(1, 33):
            ab = summary_e1['active']
            br = (eb * EB_INCREMENT * 16) // math.isqrt(ab)
            base_reward[eb-1] = br
            i_reward[eb-1] = (br * summary_e['attesting']) // ab
            t_reward[eb-1] = (br * summary_e['target'])    // ab
            h_reward[eb-1] = (br * summary_e['head'])      // ab

        chaind.load_validator_epoch_summary(e)

        e0 = e * 32
        for s in range(e0, e0 + 32): # iterate through slots in this epoch

            min_inclusion_delay = 1
            while not filled_slots[s % 32 + min_inclusion_delay]:
                min_inclusion_delay += 1

            # calculate the maximum attestation reward for this slot

            for eb in range(1, 33):
                br = base_reward[eb-1]
                max_d_reward[eb-1] = (br - br // 8) // min_inclusion_delay
                max_reward[eb-1] = i_reward[eb-1] + t_reward[eb-1] \
                                 + h_reward[eb-1] + max_d_reward[eb-1]

            # calculate attestation rewards earned/missed by each validator

            val_indices = chaind.get_scheduled_attestors(s)
            for val_index in val_indices:
                v = chaind.validators[val_index]
                eb = v['effective_balance']
                v['max_attestation_reward'] += max_reward[eb-1]
                v['attestation_slot'] = s
                br = base_reward[eb-1]

                att_reward = 0
                if v['attestation_included']:
                    att_reward += i_reward[eb-1]
                    dr = (br - br // 8) // v['inclusion_delay']
                    att_reward += dr
                    v['shortfall_delay'] += max_d_reward[eb-1] - dr
                    if v['target_correct']:
                        att_reward += t_reward[eb-1]
                    else:
                        att_reward -= br
                        v['shortfall_target'] += t_reward[eb-1] + br
                    if v['head_correct']:
                        att_reward += h_reward[eb-1]
                    else:
                        att_reward -= br
                        v['shortfall_head'] += h_reward[eb-1] + br
                else:
                    att_reward -= 3* br
                    v['shortfall_missed'] += max_reward[eb-1] + 3 * br

                v['this_att_reward'] = att_reward
                v['attestation_reward'] += att_reward

        # calculate block rewards earned/missed by each proposer

        proposers = chaind.get_shifted_proposers(e)
        for i, val_index in enumerate(proposers):
            v = chaind.validators[val_index]
            if filled_slots[i+1]:
                bal_change = chaind.get_balance_delta(val_index, e)
                props_included = 0
                for j, vi in enumerate(proposers):
                    if filled_slots[j+1] and vi == val_index:
                        props_included += 1

                att_reward = v['this_att_reward']
                block_reward = (bal_change - att_reward) // props_included
                v['block_reward'] += block_reward
            else:
                numerator = base_reward[0] * summary_e['attesting']
                est_br = numerator // (EB_INCREMENT * 8 * 32)
                v['missed_block_reward'] += est_br

        chaind.insert_epoch_extras(e)
        print(f"calculated validator epoch extras for epoch {e}", end='\r')
        e += 1
