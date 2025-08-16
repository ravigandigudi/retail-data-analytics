CREATE INDEX IF NOT EXISTS ix_df_orders_order_date ON df_orders (order_date);
CREATE INDEX IF NOT EXISTS ix_df_orders_region_product ON df_orders (region, product_id);
CREATE INDEX IF NOT EXISTS ix_df_orders_category_date ON df_orders (category, order_date);
CREATE INDEX IF NOT EXISTS ix_df_orders_subcat_date ON df_orders (sub_category, order_date);
