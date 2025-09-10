from stellar_sdk import *
import requests
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
def SendTransactions(operations):
    if len(operations) == 0:
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
        print(f"Transaction failed: {e}")

###################################################################################
# Claimable Balances Logic
###################################################################################
def GetClaimableBalances(distributor_public):
    url = f"https://horizon.stellar.org/claimable_balances?claimant={distributor_public}&limit=200"
    response = requests.get(url)
    response.raise_for_status()
    records = response.json()['_embedded']['records']

    balance_ids = []
    for record in records:
        balance_ids.append(record['id'])
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