import bitmex

from Functions.consts import *


def balances_status():
    client = bitmex.bitmex(test=TESTNET_EXCHANGE,
                           api_key=BITMEX_API_KEY,
                           api_secret=BITMEX_API_SECRET)

    if len(client.User.User_getWalletSummary().result()[0]) == 3:
        wallet_summary = client.User.User_getWalletSummary().result()[0]

        realised_pnl = wallet_summary[1]['unrealisedPnl'] / 100000000.
        total = wallet_summary[2]['walletBalance'] / 100000000.

        return total, realised_pnl
    else:
        wallet_summary = client.User.User_getWalletSummary().result()[0]

        total = wallet_summary[1]['walletBalance'] / 100000000.

        return total, 0.0


def amount_in_orders():
    client = bitmex.bitmex(test=TESTNET_EXCHANGE,
                           api_key=BITMEX_API_KEY,
                           api_secret=BITMEX_API_SECRET)

    result = client.Position.Position_get().result()

    return float(result[0][0]['currentQty'])


def has_open_positions():
    client = bitmex.bitmex(test=TESTNET_EXCHANGE,
                           api_key=BITMEX_API_KEY,
                           api_secret=BITMEX_API_SECRET)

    result = client.Position.Position_get().result()[0][0]

    if result['avgEntryPrice'] == 0:
        return False
    else:
        return True


def not_in_position():
    client = bitmex.bitmex(test=TESTNET_EXCHANGE,
                           api_key=BITMEX_API_KEY,
                           api_secret=BITMEX_API_SECRET)

    result = client.Order.Order_getOrders(filter='{"open": true}').result()

    #logger.debug("Orders: " + str(result))

    if len(result[0]) == 0:
        return True
    else:
        order_types = []
        for order in result[0]:
            order_types.append(order['ordType'])
        if len(set(order_types)) == 1 and result[0][0]['ordType'] == 'StopLimit':
            return True
        else:
            return False


def cancel_all_orders():
    client = bitmex.bitmex(test=TESTNET_EXCHANGE,
                           api_key=BITMEX_API_KEY,
                           api_secret=BITMEX_API_SECRET)

    client.Order.Order_cancelAll().result()


if __name__ == "__main__":
    cancel_all_orders()
    print(balances_status())
