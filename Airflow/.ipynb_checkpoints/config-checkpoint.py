import os

# Define credentials here
MY_SECRET_KEY = "4+g2ln00W5a9LGldgrNovGhtnNfJZu9gK1xe/xiV"
MY_API_KEY = "AKIA2N6DE7VO4J6F5VMV"

# Get credentials from environment variables if available
MY_SECRET_KEY = os.environ.get("MY_SECRET_KEY", MY_SECRET_KEY)
MY_API_KEY = os.environ.get("MY_API_KEY", MY_API_KEY)
    