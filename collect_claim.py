from stellar_sdk import Server, Keypair, TransactionBuilder, Network, ClaimClaimableBalance
from alive_progress import alive_bar
import requests
import colorama

# Account configuration
account_keypair = Keypair.from_secret('')
account_public  = account_keypair.public_key
account_secret  = account_keypair.secret

# Stellar server
server = Server(horizon_url='https://horizon.stellar.org')

# Fetch claimable balances
def get_claimable_balances_for_account(account_id):
    """Fetch all claimable balances where account_id is the sponsor."""
    claimable_records = []
    url = f"{server.horizon_url}/claimable_balances?sponsor={account_id}&limit=200"

    while url:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print("Error fetching claimable balances:", e)
            continue

        claimable_records.extend(data['_embedded']['records'])
        next_url = data['_links']['next']['href']

        # Stop if next cursor equals current cursor
        if 'cursor=' in next_url and 'cursor=' in url:
            if next_url.split('cursor=')[1].split('&')[0] == url.split('cursor=')[1].split('&')[0]:
                break

        url = next_url if next_url != url else None

    return claimable_records

def get_claimable_balance_ids(account_id):
    """Return a list of claimable balance IDs for a given account."""
    return [data['id'] for data in get_claimable_balances_for_account(account_id)]

# -----------------------------
# Collect claimable balances
# -----------------------------
def collect_claimable_balances(account_secret, balances, progress_bar=None):
    """Claim balances for the given secret key in batches of 100."""
    keypair = Keypair.from_secret(account_secret)
    public_key = keypair.public_key

    for i in range(0, len(balances), 100):
        batch = balances[i:i+100]
        fee = server.fetch_base_fee()

        while True:
            tx_builder = TransactionBuilder(
                source_account=server.load_account(public_key),
                network_passphrase=Network.PUBLIC_NETWORK_PASSPHRASE,
                base_fee=fee
            )

            for balance_id in batch:
                tx_builder.append_operation(ClaimClaimableBalance(balance_id=balance_id, source=public_key))

            if not tx_builder.operations:
                break

            tx_builder.set_timeout(120)
            transaction = tx_builder.build()
            transaction.sign(account_secret)

            try:
                server.submit_transaction(transaction)
            except Exception as error:
                print("Transaction error:", error)
            else:
                if progress_bar:
                    progress_bar(len(batch))
                break

# -----------------------------
# Main collection loop
# -----------------------------
def main_collect(account_secret):
    """Main loop to collect all claimable balances for an account."""
    balances = get_claimable_balance_ids(Keypair.from_secret(account_secret).public_key)
    with alive_bar(len(balances), title=colorama.Fore.LIGHTGREEN_EX + 'Collecting:', bar='blocks') as bar:
        collect_claimable_balances(account_secret, balances, bar)

        while True:
            balances = get_claimable_balance_ids(Keypair.from_secret(account_secret).public_key)
            if not balances:
                break
            collect_claimable_balances(account_secret, balances, bar)

# -----------------------------
# Usage
# -----------------------------
main_collect(account_secret)
