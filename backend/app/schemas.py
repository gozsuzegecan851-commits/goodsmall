from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field

class CategoryIn(BaseModel):
    id: Optional[int] = None
    name: str
    cover_image: str = ""
    sort_order: int = 100
    is_active: bool = True

class ProductSkuIn(BaseModel):
    id: Optional[int] = None
    sku_code: str = ""
    sku_name: str = ""
    spec_text: str = ""
    price_cny: Decimal = Decimal("0")
    original_price_cny: Decimal = Decimal("0")
    stock_qty: int = 0
    weight_gram: int = 0
    unit_text: str = "件"
    cover_image: str = ""
    is_active: bool = True
    sort_order: int = 100

class ProductSkuOut(ProductSkuIn):
    pass

class ProductIn(BaseModel):
    id: Optional[int] = None
    category_id: Optional[int] = None
    name: str
    subtitle: str = ""
    sku_code: str = ""
    cover_image: str = ""
    gallery_images_json: str = "[]"
    price_cny: Decimal = Decimal("0")
    original_price_cny: Decimal = Decimal("0")
    stock_qty: int = 0
    weight_gram: int = 0
    unit_text: str = "件"
    description: str = ""
    detail_html: str = ""
    is_active: bool = True
    sort_order: int = 100
    sku_list: list[ProductSkuIn] = []

class AddressIn(BaseModel):
    id: Optional[int] = None
    bot_code: str = "buyer001"
    telegram_user_id: str
    receiver_name: str
    receiver_phone: str
    province: str
    city: str
    district: str
    address_detail: str
    postal_code: str = ""
    is_default: bool = False
    remark: str = ""

class OrderCreateItem(BaseModel):
    product_id: int
    sku_id: Optional[int] = None
    qty: int = Field(default=1, ge=1)

class OrderCreateIn(BaseModel):
    bot_code: str = "buyer001"
    telegram_user_id: str
    address_id: int
    items: list[OrderCreateItem]
    buyer_remark: str = ""

class PaymentCreateIn(BaseModel):
    order_id: int
    telegram_user_id: str = ""

class BotIn(BaseModel):
    id: Optional[int] = None
    bot_code: str
    bot_token: str
    bot_type: str = "buyer"
    supplier_code: str = ""
    bot_name: str = ""
    bot_alias: str = ""
    bot_short_description: str = ""
    bot_description: str = ""
    start_welcome_text: str = ""
    avatar_image: str = ""
    is_enabled: bool = True

class SupplierIn(BaseModel):
    id: Optional[int] = None
    supplier_code: str
    supplier_name: str
    supplier_type: str = "manual"
    api_base: str = ""
    api_key: str = ""
    api_secret: str = ""
    contact_name: str = ""
    contact_phone: str = ""
    contact_tg: str = ""
    template_type: str = "standard"
    shipping_bot_code: str = ""
    is_active: bool = True
    remark: str = ""

class ProductSupplierMapIn(BaseModel):
    id: Optional[int] = None
    product_id: int
    supplier_id: int
    supplier_sku: str = ""
    priority: int = 100
    is_default: bool = False
    is_active: bool = True

class AssignSupplierIn(BaseModel):
    supplier_id: int

class PaymentAddressIn(BaseModel):
    id: Optional[int] = None
    address_label: str = ""
    address: str
    qr_image: str = ""
    is_active: bool = True
    sort_order: int = 100

class OrderShipIn(BaseModel):
    courier_company: str
    courier_code: str = ""
    tracking_no: str

class OrderRemarkIn(BaseModel):
    seller_remark: str = ""


class ChatReplyIn(BaseModel):
    text: str
    operator_name: str = "session_bot"



class ChatKeywordBlockIn(BaseModel):
    id: Optional[int] = None
    keyword: str
    match_type: str = "exact"
    match_mode: str = "exact"
    is_active: bool = True
    remark: str = ""
