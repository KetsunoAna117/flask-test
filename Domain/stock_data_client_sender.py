def map_stock_data_to_client() -> dict:
    from Repository.stock_repository import fetch_all_stock_from_db
    from Repository.stock_price_detail import fetch_last_price
    from Domain.server_date_handler import get_current_date
    from Model.Stock import StockDTO

    to_send_stock_data = []

    fetched_stock = fetch_all_stock_from_db()
    for fetched_stock_item in fetched_stock:
        fetched_price = fetch_last_price(fetched_stock_item['stock_id'], get_current_date())
        to_send_stock_data.append(
            StockDTO(
                name=fetched_stock_item['stock_code_name'], 
                price=fetched_price
            ).to_dict()
        )

    return to_send_stock_data