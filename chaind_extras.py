import json
import sys

import psycopg2
from psycopg2.extras import execute_values

FUTURE_EPOCH = 2**64 - 1 # as defined in eth2 spec
EB_INCREMENT = int(1e9)

DROP_TABLE_QUERY = "DROP TABLE IF EXISTS t_validator_epoch_extras"

CREATE_TABLE_QUERY = (
    "CREATE TABLE IF NOT EXISTS t_validator_epoch_extras ("
    "    f_epoch bigint NOT NULL, "
    "    f_validator_index bigint NOT NULL, "
    "    f_attestation_slot bigint, "
    "    f_attestation_reward bigint , "
    "    f_max_attestation_reward bigint, "
    "    f_shortfall_missed bigint, "
    "    f_shortfall_target bigint, "
    "    f_shortfall_head bigint, "
    "    f_shortfall_delay bigint, "
    "    f_block_reward bigint, "
    "    f_missed_block_reward bigint, "
    "    CONSTRAINT i_validator_epoch_extras_1 "
    "    PRIMARY KEY (f_validator_index, f_epoch) "
    ")"
)

CREATE_INDEX_QUERY = (
    "CREATE INDEX IF NOT EXISTS i_validator_epoch_extras_2 "
    "ON t_validator_epoch_extras (f_epoch)"
)

VALIDATORS_QUERY = (
    "SELECT f_index, f_activation_epoch, f_exit_epoch, f_slashed, f_public_key "
    "FROM t_validators ORDER BY f_index"
)

EPOCH_EXTRAS_QUERY = (
    "SELECT f_validator_index, f_attestation_reward, f_max_attestation_reward,"
    "    f_shortfall_missed, f_shortfall_target, f_shortfall_head,"
    "    f_shortfall_delay, f_block_reward, f_missed_block_reward "
    "FROM t_validator_epoch_extras WHERE f_epoch = %d"
)

IMPAIRED_VALIDATOR_QUERY = (
    "SELECT DISTINCT f_validator_index FROM t_validator_balances "
    "WHERE f_effective_balance <> 32000000000"
)

EFFECTIVE_BALANCE_QUERY = (
    "SELECT f_effective_balance FROM t_validator_balances "
    "WHERE f_validator_index = %d ORDER BY f_epoch"
)

EPOCH_SUMMARY_QUERY = (
    "SELECT f_active_balance, f_attesting_balance, f_target_correct_balance, "
    "    f_head_correct_balance FROM t_epoch_summaries WHERE f_epoch = %d"
)

BLOCKS_QUERY = (
    "SELECT f_slot FROM t_blocks WHERE f_slot BETWEEN %d AND %d AND f_canonical"
)

VALIDATOR_EPOCH_SUMMARY_QUERY = (
    "SELECT f_validator_index, f_proposer_duties, f_proposals_included, "
    "    f_attestation_included, f_attestation_target_correct, "
    "    f_attestation_head_correct, f_attestation_inclusion_delay "
    "FROM t_validator_epoch_summaries WHERE f_epoch = %d"
)

COMMITTEE_QUERY = (
    "SELECT f_committee FROM t_beacon_committees WHERE f_slot = %d"
)

PROPOSERS_QUERY = (
    "SELECT f_validator_index FROM t_proposer_duties "
    "WHERE f_slot BETWEEN %d AND %d ORDER BY f_slot"
)

BALANCE_CHANGE_QUERY = (
    "SELECT f_balance FROM t_validator_balances "
    "WHERE f_validator_index = %d AND f_epoch IN (%d, %d)"
    "ORDER BY f_epoch"
)

INSERT_QUERY = "INSERT INTO t_validator_epoch_extras VALUES %s"

DELETE_QUERY = "DELETE FROM t_validator_epoch_extras WHERE f_epoch = %d"

class ChainDB:
    def __init__(
        self,
        user='chain',
        host='127.0.0.1',
        database='chain',
        password='medalla',
        reset=False
    ):
        self.connection = psycopg2.connect(
            user=user, host=host, database=database, password=password
        )
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

        if reset:
            self.cursor.execute(DROP_TABLE_QUERY)

        self.cursor.execute(CREATE_TABLE_QUERY)
        self.cursor.execute(CREATE_INDEX_QUERY)

        self.cursor.execute(VALIDATORS_QUERY)
        self.validators = [{
            'index'                 : r[0],
            'activation_epoch'      : FUTURE_EPOCH if r[1] is None else r[1],
            'exit_epoch'            : FUTURE_EPOCH if r[2] is None else r[2],
            'slashed'               : r[3],
            'slashed_slot'          : None,
            'slasher'               : False,
            'redeposit'             : False,
            'pubkey'                : r[4].hex(),
            'attestation_reward'    : 0,
            'max_attestation_reward': 0,
            'shortfall_missed'      : 0,
            'shortfall_target'      : 0,
            'shortfall_head'        : 0,
            'shortfall_delay'       : 0,
            'block_reward'          : 0,
            'missed_block_reward'   : 0
        } for r in self.cursor.fetchall()]

        epoch = self.get_latest_extras_epoch()
        if epoch is not None:
            self.cursor.execute(EPOCH_EXTRAS_QUERY % self.epoch)
            for r in self.cursor.fetchall():
                self.validators[r[0]]['attestation_reward'] = r[1]
                self.validators[r[0]]['max_attestation_reward'] = r[2]
                self.validators[r[0]]['shortfall_missed'] = r[3]
                self.validators[r[0]]['shortfall_target'] = r[4]
                self.validators[r[0]]['shortfall_head'] = r[5]
                self.validators[r[0]]['shortfall_delay'] = r[6]
                self.validators[r[0]]['block_reward'] = r[7]
                self.validators[r[0]]['missed_block_reward'] = r[8]

        # get balances for impaired validators

        try:
            with open('tmp/effective_balances.json') as f:
                print("loading impaired validator balances from file")
                tmp = json.load(f)
                self.effective_balances = {}
                for k in tmp:
                    self.effective_balances[int(k)] = tmp[k]
        except FileNotFoundError:
            print("identifying impaired validators")
            self.cursor.execute(IMPAIRED_VALIDATOR_QUERY)
            impaired_validators = [r[0] for r in self.cursor.fetchall()]
            self.effective_balances = {}
            for i, validator_index in enumerate(impaired_validators):
                self.cursor.execute(EFFECTIVE_BALANCE_QUERY % validator_index)
                self.effective_balances[validator_index] = [
                    r[0] // EB_INCREMENT for r in cursor.fetchall()
                ]

            with open('tmp/effective_balances.json', 'w') as f:
                json.dump(self.effective_balances, f)

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def get_latest_block(self):
        query = "SELECT MAX(f_slot) FROM t_blocks WHERE f_canonical"
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_latest_summary_epoch(self):
        query = "SELECT MAX(f_epoch) FROM t_epoch_summaries"
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_latest_extras_epoch(self):
        query = "SELECT MAX(f_epoch) FROM t_validator_epoch_extras"
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_epoch_summary_balances(self, epoch):
        self.cursor.execute(EPOCH_SUMMARY_QUERY % epoch)
        result = self.cursor.fetchone()
        return {
            'active': result[0],
            'attesting': result[1],
            'target': result[2],
            'head': result[3]
        }

    def get_filled_slots(self, epoch):
        filled_slots = [False] * 32
        e0 = epoch * 32
        self.cursor.execute(BLOCKS_QUERY % (e0, e0 + 31))
        for r in self.cursor.fetchall():
            filled_slots[r[0] % 32] = True
        return filled_slots

    def load_validator_epoch_summary(self, epoch):
        self.cursor.execute(VALIDATOR_EPOCH_SUMMARY_QUERY % epoch)
        for r in self.cursor.fetchall():
            v = self.validators[r[0]]
            if r[0] in self.effective_balances:
                v['effective_balance'] = self.effective_balances[r[0]][
                    epoch - v['activation_epoch']
                ]
            else:
                v['effective_balance'] = 32
            v['proposer_duties'] = r[1]
            v['proposals_included'] = r[2]
            v['attestation_included'] = r[3]
            v['target_correct'] = r[4]
            v['head_correct'] = r[5]
            v['inclusion_delay'] = r[6]

    def get_scheduled_attestors(self, slot):
        self.cursor.execute(COMMITTEE_QUERY % slot)
        return [
            el for sl in self.cursor.fetchall() for ssl in sl for el in ssl
        ]

    def get_shifted_proposers(self, epoch):
        e1 = epoch * 32 + 1
        self.cursor.execute(PROPOSERS_QUERY % (e1, e1 + 31))
        return [r[0] for r in self.cursor.fetchall()]

    def get_balance_delta(self, val_index, epoch):
        params = (val_index, epoch + 1, epoch + 2)
        self.cursor.execute(BALANCE_CHANGE_QUERY % params)
        balance = [r[0] for r in self.cursor.fetchall()]
        return balance[1] - balance[0]

    def insert_epoch_extras(self, epoch):
        params = []
        for v in self.validators:
            if epoch >= v['activation_epoch'] and epoch < v['exit_epoch']:
                params.append((
                    epoch,
                    v['index'],
                    v['attestation_slot'],
                    v['attestation_reward'],
                    v['max_attestation_reward'],
                    v['shortfall_missed'],
                    v['shortfall_target'],
                    v['shortfall_head'],
                    v['shortfall_delay'],
                    v['block_reward'],
                    v['missed_block_reward']
                ))

        try:
            execute_values(self.cursor, INSERT_QUERY, params)
        except KeyboardInterrupt:
            print(f"interrupted during processing for epoch {epoch}")
            self.cursor.execute(DELETE_QUERY % epoch)
            sys.exit(0)
