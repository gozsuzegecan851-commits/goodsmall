from datetime import datetime
from decimal import Decimal
from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
)
from sqlalchemy.orm import relationship
from .db import Base

class BotConfig(Base):
    __tablename__ = "bot_configs"
    id = Column(Integer, primary_key=True)
    bot_code = Column(String(64), nullable=False, unique=True, index=True)
    bot_token = Column(String(255), nullable=False, default="")
    bot_type = Column(String(32), nullable=False, default="buyer")
    supplier_code = Column(String(64), nullable=False, default="")
    bot_name = Column(String(128), nullable=False, default="")
    bot_alias = Column(String(128), nullable=False, default="")
    bot_short_description = Column(String(120), nullable=False, default="")
    bot_description = Column(Text, nullable=False, default="")
    start_welcome_text = Column(Text, nullable=False, default="")
    avatar_image = Column(Text, nullable=False, default="")
    telegram_username = Column(String(128), nullable=False, default="")
    last_profile_sync_at = Column(DateTime, nullable=True)
    last_profile_sync_error = Column(Text, nullable=False, default="")
    is_enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ProductCategory(Base):
    __tablename__ = "product_categories"
    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False, default="")
    cover_image = Column(Text, nullable=False, default="")
    sort_order = Column(Integer, nullable=False, default=100)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey("product_categories.id"), nullable=True)
    name = Column(String(255), nullable=False, default="")
    subtitle = Column(String(255), nullable=False, default="")
    sku_code = Column(String(64), nullable=False, default="")
    cover_image = Column(Text, nullable=False, default="")
    gallery_images_json = Column(Text, nullable=False, default="[]")
    price_cny = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    original_price_cny = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    stock_qty = Column(Integer, nullable=False, default=0)
    weight_gram = Column(Integer, nullable=False, default=0)
    unit_text = Column(String(64), nullable=False, default="件")
    description = Column(Text, nullable=False, default="")
    detail_html = Column(Text, nullable=False, default="")
    is_active = Column(Boolean, nullable=False, default=True)
    sort_order = Column(Integer, nullable=False, default=100)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    category = relationship("ProductCategory")
    skus = relationship("ProductSku", back_populates="product", cascade="all, delete-orphan", order_by="ProductSku.sort_order.asc(), ProductSku.id.asc()")

class ShippingAddress(Base):
    __tablename__ = "shipping_addresses"
    id = Column(Integer, primary_key=True)
    bot_code = Column(String(64), nullable=False, default="")
    telegram_user_id = Column(String(64), nullable=False, default="", index=True)
    receiver_name = Column(String(128), nullable=False, default="")
    receiver_phone = Column(String(32), nullable=False, default="")
    province = Column(String(64), nullable=False, default="")
    city = Column(String(64), nullable=False, default="")
    district = Column(String(64), nullable=False, default="")
    address_detail = Column(Text, nullable=False, default="")
    postal_code = Column(String(32), nullable=False, default="")
    is_default = Column(Boolean, nullable=False, default=False)
    remark = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class CartItem(Base):
    __tablename__ = "cart_items"
    id = Column(Integer, primary_key=True)
    bot_code = Column(String(64), nullable=False, default="")
    telegram_user_id = Column(String(64), nullable=False, default="", index=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    qty = Column(Integer, nullable=False, default=1)
    checked = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    order_no = Column(String(64), nullable=False, unique=True, index=True)
    bot_code = Column(String(64), nullable=False, default="")
    telegram_user_id = Column(String(64), nullable=False, default="", index=True)
    customer_name = Column(String(128), nullable=False, default="")
    customer_phone = Column(String(32), nullable=False, default="")
    province = Column(String(64), nullable=False, default="")
    city = Column(String(64), nullable=False, default="")
    district = Column(String(64), nullable=False, default="")
    address_detail = Column(Text, nullable=False, default="")
    postal_code = Column(String(32), nullable=False, default="")
    goods_amount = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    shipping_fee = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    discount_amount = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    payable_amount = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    payment_method = Column(String(32), nullable=False, default="usdt_trc20")
    supplier_code = Column(String(64), nullable=False, default="")
    pay_status = Column(String(32), nullable=False, default="pending")
    order_status = Column(String(32), nullable=False, default="created")
    delivery_status = Column(String(32), nullable=False, default="not_shipped")
    payment_proof_url = Column(Text, nullable=False, default="")
    paid_at = Column(DateTime, nullable=True)
    shipped_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    courier_company = Column(String(64), nullable=False, default="")
    courier_code = Column(String(64), nullable=False, default="")
    tracking_no = Column(String(128), nullable=False, default="")
    seller_remark = Column(Text, nullable=False, default="")
    buyer_remark = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    stock_reserved = Column(Boolean, nullable=False, default=False)

class OrderItem(Base):
    __tablename__ = "order_items"
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=True)
    product_name = Column(String(255), nullable=False, default="")
    sku_code = Column(String(64), nullable=False, default="")
    cover_image = Column(Text, nullable=False, default="")
    qty = Column(Integer, nullable=False, default=1)
    unit_price = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    subtotal = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    created_at = Column(DateTime, default=datetime.utcnow)

class PaymentAddress(Base):
    __tablename__ = "payment_addresses"
    id = Column(Integer, primary_key=True)
    address_label = Column(String(128), nullable=False, default="")
    address = Column(String(255), nullable=False, default="", unique=True)
    qr_image = Column(Text, nullable=False, default="")
    is_active = Column(Boolean, nullable=False, default=True)
    sort_order = Column(Integer, nullable=False, default=100)
    last_used_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class PaymentOrder(Base):
    __tablename__ = "payment_orders"
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False, index=True)
    pay_method = Column(String(32), nullable=False, default="usdt_trc20")
    receive_address = Column(String(255), nullable=False, default="")
    expected_amount = Column(Numeric(18, 6), nullable=False, default=Decimal("0"))
    base_amount = Column(Numeric(18, 6), nullable=False, default=Decimal("0"))
    amount_offset = Column(Numeric(18, 6), nullable=False, default=Decimal("0"))
    paid_amount = Column(Numeric(18, 6), nullable=False, default=Decimal("0"))
    txid = Column(String(255), nullable=False, default="")
    from_address = Column(String(255), nullable=False, default="")
    to_address = Column(String(255), nullable=False, default="")
    confirm_status = Column(String(32), nullable=False, default="pending")
    paid_at = Column(DateTime, nullable=True)
    expired_at = Column(DateTime, nullable=True)
    raw_json = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Supplier(Base):
    __tablename__ = "suppliers"
    id = Column(Integer, primary_key=True)
    supplier_code = Column(String(64), nullable=False, unique=True, index=True)
    supplier_name = Column(String(128), nullable=False, default="")
    supplier_type = Column(String(32), nullable=False, default="manual")
    api_base = Column(Text, nullable=False, default="")
    api_key = Column(Text, nullable=False, default="")
    api_secret = Column(Text, nullable=False, default="")
    contact_name = Column(String(128), nullable=False, default="")
    contact_phone = Column(String(64), nullable=False, default="")
    contact_tg = Column(String(128), nullable=False, default="")
    template_type = Column(String(64), nullable=False, default="standard")
    shipping_bot_code = Column(String(64), nullable=False, default="")
    is_active = Column(Boolean, nullable=False, default=True)
    remark = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ProductSupplierMap(Base):
    __tablename__ = "product_supplier_map"
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False, index=True)
    supplier_id = Column(Integer, ForeignKey("suppliers.id"), nullable=False, index=True)
    supplier_sku = Column(String(128), nullable=False, default="")
    priority = Column(Integer, nullable=False, default=100)
    is_default = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class OrderFulfillment(Base):
    __tablename__ = "order_fulfillments"
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False, index=True)
    supplier_id = Column(Integer, ForeignKey("suppliers.id"), nullable=False, index=True)
    supplier_order_no = Column(String(128), nullable=False, default="")
    fulfillment_status = Column(String(32), nullable=False, default="assigned")
    assigned_at = Column(DateTime, nullable=True)
    accepted_at = Column(DateTime, nullable=True)
    shipped_at = Column(DateTime, nullable=True)
    sync_status = Column(String(32), nullable=False, default="pending")
    sync_error = Column(Text, nullable=False, default="")
    raw_json = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Shipment(Base):
    __tablename__ = "shipments"
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False, index=True)
    courier_company = Column(String(64), nullable=False, default="")
    courier_code = Column(String(64), nullable=False, default="")
    tracking_no = Column(String(128), nullable=False, default="")
    ship_status = Column(String(32), nullable=False, default="pending")
    last_trace_text = Column(Text, nullable=False, default="")
    last_trace_time = Column(DateTime, nullable=True)
    signed_at = Column(DateTime, nullable=True)
    provider_name = Column(String(64), nullable=False, default="kuaidi100")
    subscribe_status = Column(String(32), nullable=False, default="none")
    sync_status = Column(String(32), nullable=False, default="pending")
    sync_error = Column(Text, nullable=False, default="")
    last_sync_at = Column(DateTime, nullable=True)
    raw_json = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ShipmentTrace(Base):
    __tablename__ = "shipment_traces"
    id = Column(Integer, primary_key=True)
    shipment_id = Column(Integer, ForeignKey("shipments.id"), nullable=False, index=True)
    trace_time = Column(DateTime, nullable=True)
    trace_status = Column(String(64), nullable=False, default="")
    trace_text = Column(Text, nullable=False, default="")
    raw_json = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

class ShipmentImportBatch(Base):
    __tablename__ = "shipment_import_batches"
    id = Column(Integer, primary_key=True)
    batch_no = Column(String(64), nullable=False, unique=True)
    file_name = Column(String(255), nullable=False, default="")
    biz_date = Column(String(32), nullable=False, default="")
    supplier_code = Column(String(64), nullable=False, default="")
    total_rows = Column(Integer, nullable=False, default=0)
    success_rows = Column(Integer, nullable=False, default=0)
    failed_rows = Column(Integer, nullable=False, default=0)
    operator_name = Column(String(128), nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

class ShipmentImportError(Base):
    __tablename__ = "shipment_import_errors"
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("shipment_import_batches.id"), nullable=False, index=True)
    row_no = Column(Integer, nullable=False, default=0)
    order_no = Column(String(64), nullable=False, default="")
    tracking_no = Column(String(128), nullable=False, default="")
    error_message = Column(Text, nullable=False, default="")
    raw_row_json = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class ProductImportBatch(Base):
    __tablename__ = "product_import_batches"
    id = Column(Integer, primary_key=True)
    batch_no = Column(String(64), nullable=False, unique=True, index=True)
    file_name = Column(String(255), nullable=False, default="")
    operator_name = Column(String(128), nullable=False, default="")
    total_rows = Column(Integer, nullable=False, default=0)
    success_rows = Column(Integer, nullable=False, default=0)
    failed_rows = Column(Integer, nullable=False, default=0)
    warning_rows = Column(Integer, nullable=False, default=0)
    created_rows = Column(Integer, nullable=False, default=0)
    updated_rows = Column(Integer, nullable=False, default=0)
    check_images = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class ProductImportError(Base):
    __tablename__ = "product_import_errors"
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("product_import_batches.id"), nullable=False, index=True)
    row_no = Column(Integer, nullable=False, default=0)
    product_id = Column(String(64), nullable=False, default="")
    sku_code = Column(String(64), nullable=False, default="")
    product_name = Column(String(255), nullable=False, default="")
    error_message = Column(Text, nullable=False, default="")
    raw_row_json = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatKeywordBlock(Base):
    __tablename__ = "chat_keyword_blocks"
    id = Column(Integer, primary_key=True)
    keyword = Column(String(255), nullable=False, index=True)
    match_type = Column(String(32), nullable=False, default="exact")
    match_mode = Column(String(32), nullable=False, default="exact")
    is_active = Column(Boolean, nullable=False, default=True)
    remark = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BotRuntimeState(Base):
    __tablename__ = "bot_runtime_states"
    id = Column(Integer, primary_key=True)
    bot_code = Column(String(64), nullable=False, unique=True, index=True)
    bot_type = Column(String(32), nullable=False, default="buyer")
    run_status = Column(String(32), nullable=False, default="stopped")
    status_text = Column(String(255), nullable=False, default="")
    instance_id = Column(String(128), nullable=False, default="")
    last_heartbeat_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    stopped_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=False, default="")
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AdminUser(Base):
    __tablename__ = "admin_users"

    id = Column(Integer, primary_key=True)
    username = Column(String(64), nullable=False, unique=True, index=True)
    display_name = Column(String(128), nullable=False, default="")
    password_hash = Column(Text, nullable=False, default="")
    role = Column(String(32), nullable=False, default="operator")
    is_active = Column(Boolean, nullable=False, default=True)
    created_by = Column(String(64), nullable=False, default="")
    last_login_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CustomerSession(Base):
    __tablename__ = "customer_sessions"
    __table_args__ = (UniqueConstraint("bot_code", "telegram_user_id", name="uq_customer_session_bot_user"),)

    id = Column(Integer, primary_key=True)
    bot_code = Column(String(64), nullable=False, default="", index=True)
    telegram_user_id = Column(String(64), nullable=False, default="", index=True)
    telegram_chat_id = Column(String(64), nullable=False, default="")
    telegram_username = Column(String(128), nullable=False, default="")
    telegram_first_name = Column(String(128), nullable=False, default="")
    telegram_last_name = Column(String(128), nullable=False, default="")
    session_status = Column(String(32), nullable=False, default="open")
    unread_count = Column(Integer, nullable=False, default=0)
    last_message_text = Column(Text, nullable=False, default="")
    last_message_type = Column(String(32), nullable=False, default="text")
    first_customer_message_at = Column(DateTime, nullable=True)
    last_customer_message_at = Column(DateTime, nullable=True)
    last_operator_reply_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CustomerMessage(Base):
    __tablename__ = "customer_messages"

    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey("customer_sessions.id"), nullable=False, index=True)
    bot_code = Column(String(64), nullable=False, default="", index=True)
    telegram_user_id = Column(String(64), nullable=False, default="", index=True)
    direction = Column(String(32), nullable=False, default="customer")
    sender_name = Column(String(128), nullable=False, default="")
    message_type = Column(String(32), nullable=False, default="text")
    content_text = Column(Text, nullable=False, default="")
    content_json = Column(Text, nullable=False, default="{}")
    telegram_message_id = Column(String(64), nullable=False, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class AnnouncementConfig(Base):
    __tablename__ = "announcement_configs"

    id = Column(Integer, primary_key=True)
    scene = Column(String(32), nullable=False, unique=True, index=True, default="startup")
    title = Column(String(255), nullable=False, default="")
    content_text = Column(Text, nullable=False, default="")
    media_type = Column(String(32), nullable=False, default="none")
    media_url = Column(Text, nullable=False, default="")
    media_mode = Column(String(32), nullable=False, default="none")
    media_items_json = Column(Text, nullable=False, default="[]")
    text_mode = Column(String(32), nullable=False, default="caption_first")
    target_bot_types = Column(String(255), nullable=False, default="buyer")
    is_enabled = Column(Boolean, nullable=False, default=False)
    send_caption = Column(Boolean, nullable=False, default=True)
    replace_start_welcome = Column(Boolean, nullable=False, default=True)
    fallback_mode = Column(String(32), nullable=False, default="text_only")
    media_cache_json = Column(Text, nullable=False, default="[]")
    media_normalize_status = Column(String(32), nullable=False, default="pending")
    media_normalize_error = Column(Text, nullable=False, default="")
    media_cache_updated_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AnnouncementReceipt(Base):
    __tablename__ = "announcement_receipts"
    __table_args__ = (UniqueConstraint("scene", "bot_code", "telegram_chat_id", name="uq_announcement_receipt_scene_bot_chat"),)

    id = Column(Integer, primary_key=True)
    scene = Column(String(32), nullable=False, index=True, default="startup")
    bot_code = Column(String(64), nullable=False, default="", index=True)
    telegram_chat_id = Column(String(64), nullable=False, default="", index=True)
    telegram_user_id = Column(String(64), nullable=False, default="")
    target_bot_type = Column(String(32), nullable=False, default="buyer")
    sent_at = Column(DateTime, default=datetime.utcnow)



class GlobalFolderLinkConfig(Base):
    __tablename__ = "global_folder_link_configs"

    id = Column(Integer, primary_key=True)
    is_enabled = Column(Boolean, nullable=False, default=False)
    primary_button_text = Column(String(64), nullable=False, default="添加到商城文件夹")
    folder_link_url = Column(Text, nullable=False, default="")
    show_settings_button = Column(Boolean, nullable=False, default=True)
    settings_button_text = Column(String(64), nullable=False, default="打开文件夹设置")
    settings_button_url = Column(Text, nullable=False, default="tg://settings/folders")
    show_manual_hint_button = Column(Boolean, nullable=False, default=True)
    manual_hint_button_text = Column(String(64), nullable=False, default="如何手动加入机器人")
    manual_hint_text = Column(Text, nullable=False, default="已导入商城文件夹。机器人私聊请在 Telegram 内手动加入文件夹或手动置顶。")
    apply_to_bot_types = Column(Text, nullable=False, default='["buyer","shipping","session"]')
    apply_to_all_bots = Column(Boolean, nullable=False, default=True)
    status = Column(String(32), nullable=False, default="unknown")
    last_checked_at = Column(DateTime, nullable=True)
    last_check_error = Column(Text, nullable=False, default="")
    check_mode = Column(String(32), nullable=False, default="weak")
    check_interval_minutes = Column(Integer, nullable=False, default=60)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProductSku(Base):
    __tablename__ = "product_skus"
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False, index=True)
    sku_code = Column(String(64), nullable=False, default="")
    sku_name = Column(String(128), nullable=False, default="")
    spec_text = Column(String(255), nullable=False, default="")
    price_cny = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    original_price_cny = Column(Numeric(10, 2), nullable=False, default=Decimal("0"))
    stock_qty = Column(Integer, nullable=False, default=0)
    weight_gram = Column(Integer, nullable=False, default=0)
    unit_text = Column(String(64), nullable=False, default="件")
    cover_image = Column(Text, nullable=False, default="")
    is_active = Column(Boolean, nullable=False, default=True)
    sort_order = Column(Integer, nullable=False, default=100)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    product = relationship("Product", back_populates="skus")
