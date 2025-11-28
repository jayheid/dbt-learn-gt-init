{% docs order_status %}

One of the following values:

| status         | definition                                       |
| -------------- | ------------------------------------------------ |
| placed         | Order placed, not yet shipped                    |
| shipped        | Order has been shipped, not yet been delivered   |
| completed      | Order has been received by customers             |
| return pending | Customer indicated they want to return this item |
| returned       | Item has been returned                           |

{% enddocs %}

{% docs payment_method %}

One of the following values:

| Method        | definition    |
| ------------- | ------------- |
| credit_card   | Credit Card   |
| coupon        | Coupon        |
| bank_transfer | Bank transfer |
| gift_card     | Gift card     |

{% enddocs %}
