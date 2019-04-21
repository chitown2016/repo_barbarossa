
import cbpro as cbpro

api_key = "6afa8092e5e2327792be076d7ce1ea9b"
api_secret = "9LpQoq20aBy9u3zGDUQCXdjAN5QrYafCbtOW0RpZjna68WlQboWZ5yW4UfgG3n0a6d2FcB62myYefxuQDFzfSw=="
passphrase = "hzk4zwi2y7"

def get_coinbase_client(**kwargs):

    return cbpro.AuthenticatedClient(api_key,api_secret,passphrase)