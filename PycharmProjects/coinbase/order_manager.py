
from time import sleep


def send_post_only_limit_order(**kwargs):

    ticker = kwargs['ticker']
    price = kwargs['price']
    client = kwargs['client']

    if 'sleeping_seconds' in kwargs.keys():
        sleeping_seconds = kwargs['sleeping_seconds']
    else:
        sleeping_seconds = 0.5

    if 'qty_str' in kwargs.keys() and 'side' in kwargs.keys():
        qty_str = kwargs['qty_str']
        side = kwargs['side']
    else:
        side = ''
        if kwargs['qty']<0:
            side = 'sell'
        elif kwargs['qty']>0:
            side = 'buy'
        qty_str = str(abs(kwargs['qty']))

    order_price = str(kwargs['price'])
    num_trials = 0

    while True:
        order_out = client.place_limit_order(product_id=ticker, side=side,price=order_price,size=qty_str, post_only=True)

        if order_out['status']!='rejected':
            success = True
            id = order_out['id']
            break
        else:
            if num_trials>10:
                success = False
                id = ''
                break
            sleep(sleeping_seconds)

            order_book_out = client.get_product_order_book(ticker, level=1)

            if side=='sell':
                order_price = order_book_out["asks"][0][0]
            elif side=='buy':
                order_price = order_book_out["bids"][0][0]

        num_trials+=1

    return {'success': success, 'id': id}

def maintain_competitive_level4limit_order(**kwargs):

    client = kwargs['client']

    order = kwargs['order']
    ticker = order['product_id']
    side = order['side']
    size = order['size']
    price = order['price']

    if 'new_price_str' in kwargs.keys():
        new_price_str = kwargs['new_price_str']
    else:
        order_book_out = client.get_product_order_book(ticker, level=1)
        best_ask = order_book_out["asks"][0][0]
        best_bid = order_book_out["bids"][0][0]
        if side=='buy':
            new_price_str = best_bid
        elif side=='sell':
            new_price_str = best_ask

    if price!=new_price_str:
        cancel_out = client.cancel_order(order['id'])
        if type(cancel_out)==dict:
            return {'status': 'already executed','new_order_id':''}
        else:
            order_out = send_post_only_limit_order(ticker=ticker, qty_str=size, price=new_price_str, side=side,client=client)
            if not order_out['success']:
                self.log.info('Unable to send a post only order exiting the algorithm...')
                exit()
            else:
                return {'status': 'order replaced', 'new_order_id': order_out['id']}







