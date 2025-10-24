# Question: Memory-Efficient Rolling Order Book

# You are designing a real-time order book for a financial exchange. The system must handle a high volume of orders where:
# - Each order has: order_id (unique), price (float), quantity (int), timestamp (int, seconds since epoch)
# - Orders can be created, updated, or canceled.
# - Multiple orders can have the same price, and price may fluctuate within a narrow range.
# Due to memory constraints, you cannot store every single order at very fine-grained prices.

# The system should support analytics queries efficiently:
# - Total quantity at a given price or price range
# - Top-K price levels by total quantity
# - Rolling statistics like average price, total quantity over last T seconds

# APIs to Implement

# record_order(order_id: str, price: float, quantity: int, timestamp: int)
# Create a new order. If the same order_id exists, it’s considered an update.

# update_order(order_id: str, new_price: float, new_quantity: int, timestamp: int)
# Change the price or quantity of an existing order.

# cancel_order(order_id: str, timestamp: int)
# Remove the order from the order book.

# get_total_quantity(price: float) -> int
# Return total quantity at the exact price level.

# get_total_quantity_in_range(low: float, high: float) -> int
# Return total quantity for all orders in the price range [low, high].

# get_top_k_prices(k: int) -> List[Tuple[float, int]]
# Return top-K price levels by total quantity.

# Constraints

# - There can be hundreds of thousands of orders per second.
# - Price changes are continuous within a narrow range, so storing every individual order may not be feasible.
# - Must support rolling analytics over a window of the last T seconds.
# - Memory usage must remain bounded.
# - Approximation is acceptable for total quantity and top-K queries if necessary.