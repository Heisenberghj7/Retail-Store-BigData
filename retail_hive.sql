--Get How many Orders were placed
SELECT count(order_id) FROM orders;

--Get Average Revenue Per Order
SELECT
    orders.order_id,
    AVG(order_items.order_item_subtotal) AS average_revenue_per_order
FROM order_items
INNER JOIN orders ON order_items.order_item_order_id = orders.order_id
GROUP BY orders.order_id;

--Get Average Revenue Per Day Per Product
SELECT
    o.orderdate AS order_date,
    p.product_id,
    p.product_name,
    AVG(oi.order_item_subtotal) AS average_revenue_per_product
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_item_order_id
JOIN products p ON oi.order_item_product_id = p.product_id
GROUP BY o.orderdate, p.product_id, p.product_name
ORDER BY order_date, p.product_id;