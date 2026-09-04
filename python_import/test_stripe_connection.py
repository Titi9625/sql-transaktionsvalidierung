import os
import stripe
from dotenv import load_dotenv

load_dotenv()

stripe_key = os.getenv("STRIPE_SECRET_KEY")

if not stripe_key:
    raise ValueError("STRIPE_SECRET_KEY is missing in the .env file.")

if not stripe_key.startswith("sk_test_"):
    raise ValueError("Please use a Stripe TEST secret key starting with sk_test_.")

stripe.api_key = stripe_key

account = stripe.Account.retrieve()

print("Stripe API connection successful.")
print("Stripe account ID:", account.id)