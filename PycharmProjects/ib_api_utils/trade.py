
from ibapi.order import (OrderComboLeg, Order)


def ComboLimitOrder(action: str, quantity: float, limitPrice: float,
                    nonGuaranteed: bool):
    # ! [combolimit]
    order = Order()
    order.action = action
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = limitPrice
    if nonGuaranteed:
        order.smartComboRoutingParams = []
        order.smartComboRoutingParams.append(TagValue("NonGuaranteed", "1"))

    # ! [combolimit]
    return order

def ComboLimitIcebergOrder(action: str, totalQuantity: float, displaySize: int, limitPrice: float,
                    nonGuaranteed: bool):
    # ! [combolimit]
    order = Order()
    order.action = action
    order.orderType = "LMT"
    order.totalQuantity = totalQuantity
    order.DisplaySize = displaySize
    order.lmtPrice = limitPrice
    if nonGuaranteed:
        order.smartComboRoutingParams = []
        order.smartComboRoutingParams.append(TagValue("NonGuaranteed", "1"))

    # ! [combolimit]
    return order




def LimitOrder(action: str, quantity: float, limitPrice: float):

    order = Order()
    order.action = action
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = limitPrice

    return order

def MarketOrder(action: str, quantity: float):

    order = Order()
    order.action = action
    order.orderType = "MKT"
    order.totalQuantity = quantity

    return order

