from stellar_sdk import *
import requests
import time
import os
from dotenv import load_dotenv
from alive_progress import alive_bar

load_dotenv()  # This reads the .env file

server = Server(horizon_url="https://horizon.stellar.org")
network_passphrase = Network.PUBLIC_NETWORK_PASSPHRASE

# Load distributor secret from environment variable
distributor_secret = os.getenv("DISTRIBUTOR_SECRET_KEY")
if not distributor_secret:
    raise Exception("DISTRIBUTOR_SECRET_KEY environment variable is not set!")

distributor_keypair = Keypair.from_secret(distributor_secret)
distributor_public = distributor_keypair.public_key

###################################################################################
# Utility: Split into chunks
###################################################################################
def Chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

###################################################################################
# SendTransactions (reused from your code)
###################################################################################
def SendTransactions(operations, retry_count=0, max_retries=5):
    if len(operations) == 0:
        return
	
    if retry_count >= max_retries:
        print("Max retries reached. Aborting transaction.")
        return

    try:
        TransactionBuild = TransactionBuilder(
            source_account=server.load_account(distributor_public),
            network_passphrase=network_passphrase,
            base_fee=20000
        )

        for op in operations:
            TransactionBuild.append_operation(op)

        TransactionBuild.set_timeout(120)
        transaction = TransactionBuild.build()
        transaction.sign(distributor_keypair)

        server.submit_transaction(transaction)
        print("Transaction successfully submitted.")

    except Exception as e:
        if hasattr(e, 'status') and e.status == 504:
            print("504 Gateway Timeout. Retrying...")
            time.sleep(5)  # Delay before retrying
            SendTransactions(operations, retry_count + 1)
        elif (
            hasattr(e, 'extras') and 
            e.extras is not None and 
            isinstance(e.extras.get('result_codes'), dict) and 
            e.extras['result_codes'].get('transaction') == 'tx_bad_seq'
		):
            print("Bad sequence number. Reloading account and retrying...")
            time.sleep(1)  # Brief delay before retrying
            SendTransactions(operations, retry_count + 1)
        elif (
            hasattr(e, 'extras') and 
            e.extras is not None and 
            isinstance(e.extras.get('result_codes'), dict) and 
            e.extras['result_codes'].get('transaction') == 'tx_too_late'
        ):
            print("Transaction time out. Retrying...")
            time.sleep(1)  # Brief delay before retrying
            SendTransactions(operations, retry_count + 1)
        elif (
            hasattr(e, 'extras') and 
            e.extras is not None and 
            isinstance(e.extras.get('result_codes'), dict) and 
            e.extras['result_codes'].get('transaction') == 'tx_insufficient_fee'
        ):
            print("Gas Fee is too high now. Retrying after 5 seconds ...")
            time.sleep(5)  # Brief delay before retrying
            SendTransactions(operations, retry_count + 1) 
        elif (
            hasattr(e, 'extras') and 
            e.extras is not None and 
            isinstance(e.extras.get('result_codes'), dict) and 
            e.extras['result_codes'].get('transaction') == 'tx_failed' and 
            e.extras['result_codes'].get('operations') and 
            len(e.extras['result_codes'].get('operations')) > 0
        ):
            # BEGIN operations_error_check			
            ops = e.extras['result_codes']['operations']
            # BEGIN operations_error_check_REMOVE_OUT_op_no_trust
            if 'op_no_trust' in ops:
                indexes=[]
                for index, value in enumerate(ops):
                    if value == 'op_no_trust':
                        indexes.append(index)
                    
                for index in sorted(indexes, reverse=True):
                    del operations[index]

                if len(operations) > 0:
                    SendTransactions(operations, retry_count + 1)
                else:
                    error_message = f"Transaction failed: Receiver accounts did not set Trust line with asset"
                    print(error_message)
			# END operations_error_check_REMOVE_OUT_op_no_trust
            elif 'op_underfunded' in ops:				
                error_message = f"Transaction failed: token amount is insufficient in distribution account."
                print(error_message)
            else:				
                error_message = f"Transaction failed: {e}"
                print(error_message)
			# END operations_error_check
        else:
            error_message = f"Transaction failed: {e}"
            print(error_message)

###################################################################################
# Predicate check
###################################################################################
def is_predicate_true(predicate):
    """Return True if predicate allows claim now."""
    if not predicate:
        return True  # empty predicate is unconditional

    if predicate.get('unconditional', False):
        return True

    if 'and' in predicate:
        return all(is_predicate_true(p) for p in predicate['and'])

    if 'or' in predicate:
        return any(is_predicate_true(p) for p in predicate['or'])

    if 'not' in predicate:
        return not is_predicate_true(predicate['not'])

    if 'before' in predicate:
        return time.time() < predicate['before']

    if 'after' in predicate:
        return time.time() > predicate['after']

    return False  # fallback for unknown predicate

###################################################################################
# Fetch claimable balances for distributor
###################################################################################
def GetClaimableBalances(distributor_public):
    balance_ids = []
    url = f"https://horizon.stellar.org/claimable_balances?claimant={distributor_public}&limit=200"
    while url:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        for record in data['_embedded']['records']:
            for claimant in record['claimants']:
                if claimant['destination'] == distributor_public:
                    if is_predicate_true(claimant.get('predicate', {})):
                        balance_ids.append(record['id'])
        next_url = data['_links'].get('next', {}).get('href')
        self_url = data['_links'].get('next', {}).get('href')
        if next_url == self_url or not next_url:
            break  # stop if next is same as current or invalid url
        # Now, set url to the next url
        url = next_url
    return balance_ids

def ReclaimBalances(balance_ids):
    operations = []
    for bid in balance_ids:
        op = ClaimClaimableBalance(balance_id=bid)
        operations.append(op)

    print(f"Starting reclaim of {len(operations)} balances...")
    with alive_bar(len(operations), title='Reclaiming Balances') as bar:
        for chunk in Chunker(operations, 100):
            SendTransactions(chunk)
            bar(len(chunk))

def AutoReclaimExpiredBalances():
    print("Checking for expired claimable balances...")
    try:
        balance_ids = GetClaimableBalances(distributor_public)
        if not balance_ids:
            print("No claimable balances to reclaim.")
            return

        print(f"Found {len(balance_ids)} reclaimable balances.")
        ReclaimBalances(balance_ids)
        print("Reclaim finished.")
    except Exception as e:
        print(f"Error while reclaiming balances: {e}")

###################################################################################
# Main logic (run on demand)
###################################################################################
def Main():
    AutoReclaimExpiredBalances()

if __name__ == "__main__":
    Main()