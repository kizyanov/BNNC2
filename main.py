"""BNNC2 trading bot for binance."""

import asyncio
import socket
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, Decimal
from hashlib import sha256
from hmac import HMAC
from hmac import new as hmac_new
from os import environ
from ssl import SSLError
from time import time
from typing import Any, Self
from urllib.parse import urljoin

from aiohttp import ClientConnectorError, ClientSession
from dacite import (
    ForwardReferenceError,
    MissingValueError,
    StrictUnionMatchError,
    UnexpectedDataError,
    UnionMatchError,
    WrongTypeError,
    from_dict,
)
from loguru import logger
from orjson import JSONDecodeError, JSONEncodeError, dumps, loads
from result import Err, Ok, Result, do, do_async
from websockets import ClientConnection, connect
from websockets import exceptions as websockets_exceptions


@dataclass(frozen=True)
class TelegramSendMsg:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        ok: bool = field(default=False)


@dataclass(frozen=True)
class WsEvent:
    """Universal event.

    https://developers.binance.com/docs/margin_trading/trade-data-stream/Event-Order-Update
    https://developers.binance.com/docs/margin_trading/trade-data-stream/Event-Account-Update
    """

    @dataclass(frozen=True)
    class Res:
        """."""

        @dataclass(frozen=True)
        class Bs:
            """."""

            a: str = field(default="")
            f: str = field(default="")
            l: str = field(default="")

        e: str = field(default="")
        E: int = field(default=0)
        u: int = field(default=0)
        B: list[Bs] = field(default_factory=list[Bs])
        s: str = field(default="")
        p: str = field(default="")
        X: str = field(default="")


@dataclass(frozen=True)
class SapiV1MarginAccountGET:
    """https://developers.binance.com/docs/margin_trading/account/Query-Cross-Margin-Account-Details."""

    @dataclass(frozen=True)
    class Res:
        """."""

        @dataclass(frozen=True)
        class Assert:
            """.

            netAsset = free + locked - borrowed - interest
            """

            asset: str = field(default="")
            borrowed: str = field(default="")
            free: str = field(default="")
            interest: str = field(default="")
            locked: str = field(default="")
            netAsset: str = field(default="")

        userAssets: list[Assert] = field(default_factory=list[Assert])


@dataclass(frozen=True)
class SapiV1UserDataStream:
    """https://developers.binance.com/docs/margin_trading/trade-data-stream/Start-Margin-User-Data-Stream."""

    @dataclass(frozen=True)
    class Res:
        """."""

        listenKey: str = field(default="")


@dataclass(frozen=True)
class SapiV1MarginOrderPOST:
    """https://developers.binance.com/docs/margin_trading/trade/Margin-Account-New-Order#http-request."""

    @dataclass(frozen=True)
    class Res:
        """."""

        symbol: str = field(default="")
        clientOrderId: str = field(default="")


@dataclass(frozen=True)
class SapiV1MarginOrderDELETE:
    """https://developers.binance.com/docs/margin_trading/trade/Margin-Account-Cancel-Order."""

    @dataclass(frozen=True)
    class Res:
        """."""

        symbol: str = field(default="")
        status: str = field(default="")


@dataclass(frozen=True)
class SapiV1MarginOpenOrdersGET:
    """."""

    @dataclass(frozen=True)
    class Res:
        """."""

        orderId: int = field(default=0)


@dataclass(frozen=True)
class ApiV3ExchangeInfoGET:
    """."""

    @dataclass(frozen=True)
    class Res:
        """."""

        @dataclass(frozen=True)
        class Symbol:
            """."""

            @dataclass(frozen=True)
            class Filter:
                """."""

                filterType: str = field(default="")
                tickSize: str = field(default="")
                stepSize: str = field(default="")
                minQty: str = field(default="")

            symbol: str = field(default="")
            baseAsset: str = field(default="")
            quoteAsset: str = field(default="")
            isMarginTradingAllowed: bool = field(default=False)

            filters: list[Filter] = field(default_factory=list[Filter])

        symbols: list[Symbol] = field(default_factory=list[Symbol])


@dataclass(frozen=True)
class ApiV3TickerPrice:
    """https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#symbol-price-ticker."""

    @dataclass(frozen=True)
    class Res:
        """."""

        symbol: str = field(default="")
        price: str = field(default="")


@dataclass
class Book:
    """."""

    balance: Decimal = field(default=Decimal("0"))
    last_price: Decimal = field(default=Decimal("0"))
    baseincrement: Decimal = field(default=Decimal("0"))
    priceincrement: Decimal = field(default=Decimal("0"))
    baseminsize: Decimal = field(default=Decimal("0"))


class BNNC:
    """Main class collect all logic."""

    def __init__(self: Self) -> None:
        """Init settings."""
        # All about excange
        self.KEY = self.get_env("KEY").unwrap()
        self.SECRET = self.get_env("SECRET").unwrap()
        self.BASE_URL = self.get_env("BASE_URL").unwrap()

        # all about tokens
        self.ALL_CURRENCY = self.get_list_env("ALLCURRENCY").unwrap()
        self.IGNORECURRENCY = self.get_list_env("IGNORECURRENCY").unwrap()
        self.BASE_KEEP = Decimal(self.get_env("BASE_KEEP").unwrap())

        # All about tlg
        self.TELEGRAM_BOT_API_KEY = self.get_env("TELEGRAM_BOT_API_KEY").unwrap()
        self.TELEGRAM_BOT_CHAT_ID = self.get_list_env("TELEGRAM_BOT_CHAT_ID").unwrap()

        # db store
        self.PG_USER = self.get_env("PG_USER").unwrap()
        self.PG_PASSWORD = self.get_env("PG_PASSWORD").unwrap()
        self.PG_DATABASE = self.get_env("PG_DATABASE").unwrap()
        self.PG_HOST = self.get_env("PG_HOST").unwrap()
        self.PG_PORT = self.get_env("PG_PORT").unwrap()

        logger.success("Settings are OK!")

    def logger_success[T](self: Self, data: T) -> Result[T, Exception]:
        """Success logger for Pipes."""
        logger.success(data)
        return Ok(data)

    def logger_info[T](self: Self, data: T) -> Result[T, Exception]:
        """Info logger for Pipes."""
        logger.info(data)
        return Ok(data)

    def logger_exception[T](self: Self, data: T) -> Result[T, Exception]:
        """Exception logger for Pipes."""
        logger.exception(data)
        return Ok(data)

    def encode(self: Self, data: str) -> Result[bytes, Exception]:
        """Return Ok(bytes) from str data."""
        try:
            return Ok(data.encode())
        except AttributeError as exc:
            logger.exception(exc)
            return Err(exc)

    def decode(self: Self, data: bytes) -> Result[str, Exception]:
        """Return Ok(str) from bytes data."""
        try:
            return Ok(data.decode())
        except AttributeError as exc:
            logger.exception(exc)
            return Err(exc)

    def convert_to_int(self: Self, data: float) -> Result[int, Exception]:
        """Convert data to int."""
        try:
            return Ok(int(data))
        except ValueError as exc:
            logger.exception(exc)
            return Err(exc)

    def _env_convert_to_list(self: Self, data: str) -> Result[list[str], Exception]:
        """Split str by ',' character."""
        return Ok(data.split(","))

    def get_list_env(self: Self, key: str) -> Result[list[str], Exception]:
        """Get value from ENV in list[str] format.

        in .env
        KEYS=1,2,3,4,5,6

        to
        KEYS = ['1','2','3','4','5','6']
        """
        return do(
            Ok(value_in_list)
            for value_by_key in self.get_env(key)
            for value_in_list in self._env_convert_to_list(value_by_key)
        )

    def get_env(self: Self, key: str) -> Result[str, ValueError]:
        """Just get key from EVN."""
        try:
            return Ok(environ[key])
        except ValueError as exc:
            logger.exception(exc)
            return Err(exc)

    def create_book(self: Self) -> Result[None, Exception]:
        """Build own structure.

        build inside book for tickets
        book
        {
            "ADA": {
                "balance": Decimal,
                "last": Decimal,
                "baseincrement": Decimal,
                "priceincrement": Decimal,
            },
            "JUP": {
                "balance": Decimal,
                "last": Decimal,
                "baseincrement": Decimal,
                "priceincrement": Decimal,
            },
            "SOL": {
                "balance": Decimal,
                "last": Decimal,
                "baseincrement": Decimal,
                "priceincrement": Decimal,
            },
            "BTC": {
                "balance": Decimal,
                "last": Decimal,
                "baseincrement": Decimal,
                "priceincrement": Decimal,
            }
        }
        book_orders = {
            "ADA": {
                "sellorder": "",
                "buyorder": ""
            },
            "JUP": {
                "sellorder": "",
                "buyorder": ""
            },
            "SOL": {
                "sellorder": "",
                "buyorder": ""
            },
            "BTC": {
                "sellorder": "",
                "buyorder": ""
            }
        }
        """
        self.book: dict[str, Book] = {
            ticket: Book() for ticket in self.ALL_CURRENCY if isinstance(ticket, str)
        }
        self.book_orders: dict[str, list[str]] = {
            ticket: [] for ticket in self.ALL_CURRENCY if isinstance(ticket, str)
        }
        return Ok(None)

    def dumps_dict_to_bytes(
        self: Self,
        data: dict[str, Any],
    ) -> Result[bytes, Exception]:
        """Dumps dict to bytes[json].

        {"qaz":"edc"} -> b'{"qaz":"wsx"}'
        """
        try:
            return Ok(dumps(data))
        except JSONEncodeError as exc:
            logger.exception(exc)
            return Err(exc)

    async def request(
        self: Self,
        url: str,
        method: str,
        headers: dict[str, str],
        data: bytes | None = None,
    ) -> Result[bytes, Exception]:
        """Base http request."""
        try:
            async with (
                ClientSession(
                    headers=headers,
                ) as session,
                session.request(
                    method,
                    url,
                    data=data,
                ) as response,
            ):
                res = await response.read()  # bytes
                logger.success(f"{response.status}:{method}:{url}")
                return Ok(res)
        except ClientConnectorError as exc:
            logger.exception(exc)
            return Err(exc)

    def get_full_url(
        self: Self,
        base_url: str,
        next_url: str,
    ) -> Result[str, Exception]:
        """Right cancatinate base url and method url."""
        return Ok(urljoin(base_url, next_url))

    def get_time(self: Self) -> Result[float, Exception]:
        """Get now time as float."""
        return Ok(time())

    def get_now_time(self: Self) -> Result[int, Exception]:
        """Get now time for encrypted data."""
        return do(
            Ok(time_now_in_int * 1000)
            for time_now in self.get_time()
            for time_now_in_int in self.convert_to_int(time_now)
        )

    def parse_bytes_to_dict(
        self: Self,
        data: bytes | str,
    ) -> Result[dict[str, Any], Exception]:
        """Parse bytes[json] to dict.

        b'{"qaz":"wsx"}' -> {"qaz":"wsx"}
        """
        try:
            return Ok(loads(data))
        except JSONDecodeError as exc:
            logger.exception(exc)
            return Err(exc)

    def parse_bytes_to_list(
        self: Self,
        data: bytes | str,
    ) -> Result[list[Any], Exception]:
        """Parse bytes[json] to list.

        b'[{"qaz":"wsx"}]' -> [{"qaz":"wsx"}]
        """
        try:
            return Ok(loads(data))
        except JSONDecodeError as exc:
            logger.exception(exc)
            return Err(exc)

    def get_headers_auth(self: Self) -> Result[dict[str, str], Exception]:
        """Get headers for http request."""
        return Ok(
            {
                "X-MBX-APIKEY": self.KEY,
            },
        )

    def get_init_http_params(self: Self) -> Result[dict[str, str | int], Exception]:
        """Get init params for request.

        return {"recvWindows": 10000, "timestamp": 123131231}
        """
        return do(
            Ok({"recvWindows": 10000, "timestamp": now_time})
            for now_time in self.get_now_time()
        )

    def get_http_params_as_str(
        self: Self,
        params: dict[str, str | int],
    ) -> Result[str, Exception]:
        """Get url params in str.

        if params is empty -> ''
        if params not empty -> ?foo=bar&zoo=net
        """
        return Ok("&".join([f"{key}={params[key]}" for key in sorted(params)]))

    def get_default_hmac(
        self: Self,
        secret: bytes,
        data: bytes,
    ) -> Result[HMAC, Exception]:
        """Get default HMAC."""
        return Ok(hmac_new(secret, data, sha256))

    def convert_hmac_to_hexdigest(
        self: Self,
        hmac_object: HMAC,
    ) -> Result[str, Exception]:
        """Convert HMAC to digest."""
        return Ok(hmac_object.hexdigest())

    def encrypt_data(self: Self, secret: bytes, data: bytes) -> Result[str, Exception]:
        """Encrypt `data` to hmac."""
        return do(
            Ok(hmac_data)
            for hmac_object in self.get_default_hmac(secret, data)
            for hmac_data in self.convert_hmac_to_hexdigest(hmac_object)
        )

    def get_signature(
        self: Self,
        params: dict[str, str | int],
    ) -> Result[str, Exception]:
        """."""
        return do(
            Ok(sign_params)
            for params_string in self.get_http_params_as_str(params)
            for params_string_bytes in self.encode(params_string)
            for secret_bytes in self.encode(self.SECRET)
            for sign_params in self.encrypt_data(secret_bytes, params_string_bytes)
        )

    def convert_to_dataclass_from_list[T](
        self: Self,
        data_class: type[T],
        data: list[dict[str, Any]],
    ) -> Result[list[T], Exception]:
        """Convert list object to list dataclasses."""
        result: list[T] = []
        for obj in data:
            match self.convert_to_dataclass_from_dict(
                data_class,
                obj,
            ):
                case Ok(res):
                    result.append(res)
                case Err(exc):
                    self.logger_exception(exc)
                    return Err(exc)
        return Ok(result)

    def convert_to_dataclass_from_dict[T](
        self: Self,
        data_class: type[T],
        data: dict[str, Any],
    ) -> Result[T, Exception]:
        """Convert dict to dataclass."""
        try:
            return Ok(
                from_dict(
                    data_class=data_class,
                    data=data,
                ),
            )
        except (
            WrongTypeError,
            MissingValueError,
            UnionMatchError,
            StrictUnionMatchError,
            UnexpectedDataError,
            ForwardReferenceError,
        ) as exc:
            return Err(exc)

    def union_params(
        self: Self,
        data: dict[str, str | int],
        unioned_data: dict[str, str | int],
    ) -> Result[dict[str, str | int], Exception]:
        """."""
        try:
            data.update(unioned_data)
            return Ok(data)
        except (TypeError, AttributeError) as exc:
            return Err(exc)

    def add_signature_to_params(
        self: Self,
        params: dict[str, str | int],
        signature: str,
    ) -> Result[dict[str, str | int], Exception]:
        """."""
        try:
            params.update({"signature": signature})
            return Ok(params)
        except (TypeError, AttributeError) as exc:
            return Err(exc)

    def cancatinate_str(self: Self, *args: str) -> Result[str, Exception]:
        """Cancatinate to str."""
        try:
            return Ok("".join(args))
        except TypeError as exc:
            logger.exception(exc)
            return Err(exc)

    async def post_sapi_v1_margin_order(
        self: Self,
        user_params: dict[str, str | int],
    ) -> Result[SapiV1MarginOrderPOST.Res, Exception]:
        """Get all open orders.

        https://developers.binance.com/docs/margin_trading/trade/Margin-Account-New-Order
        """
        uri = "/sapi/v1/margin/order"
        method = "POST"
        return await do_async(
            Ok(data_dataclass)
            for init_params in self.get_init_http_params()
            for union_params in self.union_params(init_params, user_params)
            for sign_union_params in self.get_signature(union_params)
            for complete_params in self.add_signature_to_params(
                union_params,
                sign_union_params,
            )
            for complete_params_str in self.get_http_params_as_str(complete_params)
            for params_str in self.cancatinate_str(
                f"{uri}?",
                complete_params_str,
            )
            for full_url in self.get_full_url(
                self.BASE_URL,
                params_str,
            )
            for headers in self.get_headers_auth()
            for response_bytes in await self.request(
                method=method,
                url=full_url,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for _ in self.logger_info(response_dict)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                SapiV1MarginOrderPOST.Res,
                response_dict,
            )
        )

    async def delete_sapi_v1_margin_order(
        self: Self,
        user_params: dict[str, str | int],
    ) -> Result[SapiV1MarginOrderDELETE.Res, Exception]:
        """Cancel open order.

        https://developers.binance.com/docs/margin_trading/trade/Margin-Account-Cancel-Order
        """
        uri = "/sapi/v1/margin/order"
        method = "DELETE"
        return await do_async(
            Ok(data_dataclass)
            for init_params in self.get_init_http_params()
            for union_params in self.union_params(init_params, user_params)
            for sign_union_params in self.get_signature(union_params)
            for complete_params in self.add_signature_to_params(
                union_params,
                sign_union_params,
            )
            for complete_params_str in self.get_http_params_as_str(complete_params)
            for params_str in self.cancatinate_str(
                f"{uri}?",
                complete_params_str,
            )
            for full_url in self.get_full_url(
                self.BASE_URL,
                params_str,
            )
            for headers in self.get_headers_auth()
            for response_bytes in await self.request(
                method=method,
                url=full_url,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                SapiV1MarginOrderDELETE.Res,
                response_dict,
            )
        )

    async def massive_cancel_order(
        self: Self,
        data: list[SapiV1MarginOpenOrdersGET.Res],
    ) -> Result[None, Exception]:
        """Cancel all open order."""
        for order in data:
            await self.delete_sapi_v1_margin_order({"orderId": order.orderId})
        return Ok(None)

    async def get_sapi_v1_margin_account(
        self: Self,
        user_params: dict[str, str | int],
    ) -> Result[SapiV1MarginAccountGET.Res, Exception]:
        """Get all account balance.

        https://developers.binance.com/docs/margin_trading/account/Query-Cross-Margin-Account-Details
        """
        uri = "/sapi/v1/margin/account"
        method = "GET"
        return await do_async(
            Ok(data_dataclass)
            for init_params in self.get_init_http_params()
            for union_params in self.union_params(init_params, user_params)
            for sign_union_params in self.get_signature(union_params)
            for complete_params in self.add_signature_to_params(
                union_params,
                sign_union_params,
            )
            for complete_params_str in self.get_http_params_as_str(complete_params)
            for params_str in self.cancatinate_str(
                f"{uri}?",
                complete_params_str,
            )
            for full_url in self.get_full_url(
                self.BASE_URL,
                params_str,
            )
            for headers in self.get_headers_auth()
            for response_bytes in await self.request(
                method=method,
                url=full_url,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                SapiV1MarginAccountGET.Res,
                response_dict,
            )
        )

    async def get_api_v3_ticker_price(
        self: Self,
        user_params: dict[str, str | int],
    ) -> Result[list[ApiV3TickerPrice.Res], Exception]:
        """Get ticker price.

        https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#symbol-price-ticker
        """
        uri = "/api/v3/ticker/price"
        method = "GET"
        return await do_async(
            Ok(data_dataclass)
            for complete_params_str in self.get_http_params_as_str(user_params)
            for params_str in self.cancatinate_str(
                f"{uri}?",
                complete_params_str,
            )
            for full_url in self.get_full_url(
                self.BASE_URL,
                params_str,
            )
            for headers in self.get_headers_auth()
            for response_bytes in await self.request(
                method=method,
                url=full_url,
                headers=headers,
            )
            for response_list in self.parse_bytes_to_list(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_list(
                ApiV3TickerPrice.Res,
                response_list,
            )
        )

    async def get_api_v3_exchange_info(
        self: Self,
    ) -> Result[ApiV3ExchangeInfoGET.Res, Exception]:
        """Get symbol information.

        https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#exchange-information
        """
        uri = "/api/v3/exchangeInfo"
        method = "GET"
        return await do_async(
            Ok(data_dataclass)
            for full_url in self.get_full_url(
                self.BASE_URL,
                uri,
            )
            for response_bytes in await self.request(
                method=method,
                url=full_url,
                headers={},
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV3ExchangeInfoGET.Res,
                response_dict,
            )
        )

    def _fill_balance(
        self: Self,
        data: SapiV1MarginAccountGET.Res,
    ) -> Result[None, Exception]:
        """."""
        for ticket in data.userAssets:
            if ticket.asset in self.book:
                self.book[ticket.asset].balance = Decimal(ticket.netAsset)
        return Ok(None)

    async def fill_balance(self: Self) -> Result[None, Exception]:
        """Fill all balance by ENVs."""
        return await do_async(
            Ok(None)
            for balance_accounts in await self.get_sapi_v1_margin_account({})
            for _ in self._fill_balance(balance_accounts)
        )

    def _fill_base_increment(
        self: Self,
        data: ApiV3ExchangeInfoGET.Res,
    ) -> Result[None, Exception]:
        """Fill base increment by each token."""
        for symbol in data.symbols:
            if symbol.baseAsset in self.book and symbol.quoteAsset == "USDT":
                for one_filter in symbol.filters:
                    if one_filter.filterType == "LOT_SIZE":
                        self.book[symbol.baseAsset].baseincrement = Decimal(
                            str(float(one_filter.stepSize)),
                        )
        return Ok(None)

    def _fill_price_increment(
        self: Self,
        data: ApiV3ExchangeInfoGET.Res,
    ) -> Result[None, Exception]:
        """Fill price increment by each token."""
        for symbol in data.symbols:
            if symbol.baseAsset in self.book and symbol.quoteAsset == "USDT":
                for one_filter in symbol.filters:
                    if one_filter.filterType == "PRICE_FILTER":
                        self.book[symbol.baseAsset].priceincrement = Decimal(
                            str(float(one_filter.tickSize)),
                        )
        return Ok(None)

    def _fill_min_base_increment(
        self: Self,
        data: ApiV3ExchangeInfoGET.Res,
    ) -> Result[None, Exception]:
        """."""
        for symbol in data.symbols:
            if symbol.baseAsset in self.book and symbol.quoteAsset == "USDT":
                for one_filter in symbol.filters:
                    if one_filter.filterType == "LOT_SIZE":
                        self.book[symbol.baseAsset].baseminsize = Decimal(
                            str(float(one_filter.minQty)),
                        )
        return Ok(None)

    async def fill_increment(self: Self) -> Result[None, Exception]:
        """Fill increment from api."""
        return await do_async(
            Ok(None)
            for ticket_info in await self.get_api_v3_exchange_info()
            for _ in self._fill_base_increment(ticket_info)
            for _ in self._fill_price_increment(ticket_info)
            for _ in self._fill_min_base_increment(ticket_info)
        )

    def _fill_last_price(
        self: Self,
        data: list[ApiV3TickerPrice.Res],
    ) -> Result[None, Exception]:
        """Fill last price for each token."""
        for ticket in data:
            symbol = ticket.symbol.replace("USDT", "")
            if symbol in self.book:
                self.book[symbol].last_price = Decimal(ticket.price)

        return Ok(None)

    async def fill_last_price(self: Self) -> Result[None, Exception]:
        """Fill last price for first order init."""
        return await do_async(
            Ok(None)
            for market_prices in await self.get_api_v3_ticker_price({})
            for _ in self._fill_last_price(market_prices)
        )

    def check_telegram_response(
        self: Self,
        data: TelegramSendMsg.Res,
    ) -> Result[None, Exception]:
        """Check telegram response on msg."""
        if data.ok:
            return Ok(None)
        return Err(Exception(f"{data}"))

    def get_telegram_msg(
        self: Self,
        chat_id: str,
        data: str,
    ) -> Result[dict[str, bool | str], Exception]:
        """Get msg for telegram in dict."""
        return Ok(
            {
                "chat_id": chat_id,
                "parse_mode": "HTML",
                "disable_notification": True,
                "text": data,
            },
        )

    def get_telegram_url(self: Self) -> Result[str, Exception]:
        """Get url for send telegram msg."""
        return Ok(
            f"https://api.telegram.org/bot{self.TELEGRAM_BOT_API_KEY}/sendMessage",
        )

    async def send_msg_to_each_chat_id(
        self: Self,
        chat_ids: list[str],
        data: str,
    ) -> Result[TelegramSendMsg.Res, Exception]:
        """Send msg for each chat id."""
        method = "POST"
        for chat in chat_ids:
            await do_async(
                Ok(result)
                for telegram_url in self.get_telegram_url()
                for msg in self.get_telegram_msg(chat, data)
                for msg_bytes in self.dumps_dict_to_bytes(msg)
                for response_bytes in await self.request(
                    url=telegram_url,
                    method=method,
                    headers={
                        "Content-Type": "application/json",
                    },
                    data=msg_bytes,
                )
                for response_dict in self.parse_bytes_to_dict(response_bytes)
                for data_dataclass in self.convert_to_dataclass_from_dict(
                    TelegramSendMsg.Res,
                    response_dict,
                )
                for result in self.check_telegram_response(data_dataclass)
            )
        return Ok(TelegramSendMsg.Res())

    def get_chat_ids_for_telegram(self: Self) -> Result[list[str], Exception]:
        """Get list chat id for current send."""
        return Ok(self.TELEGRAM_BOT_CHAT_ID)

    async def send_telegram_msg(self: Self, data: str) -> Result[None, Exception]:
        """Send msg to telegram."""
        match await do_async(
            Ok(None)
            for chat_ids in self.get_chat_ids_for_telegram()
            for _ in await self.send_msg_to_each_chat_id(chat_ids, data)
        ):
            case Err(exc):
                logger.exception(exc)
        return Ok(None)

    async def get_sapi_v1_margin_open_orders(
        self: Self,
        user_params: dict[str, str | int],
    ) -> Result[list[SapiV1MarginOpenOrdersGET.Res], Exception]:
        """Get all open orders.

        https://developers.binance.com/docs/margin_trading/trade/Query-Margin-Account-Open-Orders
        """
        uri = "/sapi/v1/margin/openOrders"
        method = "GET"
        return await do_async(
            Ok(data_dataclass)
            for init_params in self.get_init_http_params()
            for union_params in self.union_params(init_params, user_params)
            for sign_union_params in self.get_signature(union_params)
            for complete_params in self.add_signature_to_params(
                union_params,
                sign_union_params,
            )
            for complete_params_str in self.get_http_params_as_str(complete_params)
            for params_str in self.cancatinate_str(
                f"{uri}?",
                complete_params_str,
            )
            for full_url in self.get_full_url(
                self.BASE_URL,
                params_str,
            )
            for headers in self.get_headers_auth()
            for response_bytes in await self.request(
                method=method,
                url=full_url,
                headers=headers,
            )
            for response_list in self.parse_bytes_to_list(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_list(
                SapiV1MarginOpenOrdersGET.Res,
                response_list,
            )
        )

    def show_usdt_count(self: Self) -> Result[None, Exception]:
        """Log usdt count in each token."""
        ss = Decimal("0")
        for index, ticket in enumerate(self.book, 1):
            self.logger_success(
                f"{index} {ticket}:{self.book[ticket].balance * self.book[ticket].last_price:.2f}",
            )
            ss += self.book[ticket].balance * self.book[ticket].last_price
        self.logger_success(f"{ss:.2f}")
        return Ok(None)

    async def pre_init(self: Self) -> Result[Self, Exception]:
        """Pre-init.

        get all open orders
        close all open orders
        get balance by  all tickets
        get increment by all tickets
        """
        return await do_async(
            Ok(self)
            for _ in self.create_book()
            for orders_for_cancel in await self.get_sapi_v1_margin_open_orders({})
            for _ in await self.massive_cancel_order(orders_for_cancel)
            for _ in await self.fill_balance()
            for _ in await self.fill_increment()
            for _ in await self.fill_last_price()
            for _ in self.show_usdt_count()
        )

    async def get_sapi_v1_user_data_stream(
        self: Self,
        user_params: dict[str, str | int],
    ) -> Result[SapiV1UserDataStream.Res, Exception]:
        """Get listen key for websocket.

        https://developers.binance.com/docs/margin_trading/trade-data-stream/Start-Margin-User-Data-Stream
        """
        uri = "/sapi/v1/userDataStream"
        method = "POST"
        return await do_async(
            Ok(data_dataclass)
            for init_params in self.get_init_http_params()
            for union_params in self.union_params(init_params, user_params)
            for sign_union_params in self.get_signature(union_params)
            for complete_params in self.add_signature_to_params(
                union_params,
                sign_union_params,
            )
            for complete_params_str in self.get_http_params_as_str(complete_params)
            for params_str in self.cancatinate_str(
                f"{uri}?",
                complete_params_str,
            )
            for full_url in self.get_full_url(
                self.BASE_URL,
                params_str,
            )
            for headers in self.get_headers_auth()
            for response_bytes in await self.request(
                method=method,
                url=full_url,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                SapiV1UserDataStream.Res,
                response_dict,
            )
        )

    def export_listen_key(
        self: Self,
        data: SapiV1UserDataStream.Res,
    ) -> Result[str, Exception]:
        """Export listenKey from SapiV1UserDataStream."""
        return Ok(data.listenKey)

    def events_balance(self: Self, data: WsEvent.Res) -> Result[None, Exception]:
        """Event is balance."""
        for symbol in data.B:
            if symbol.a in self.book:
                self.book[symbol.a].balance = Decimal(symbol.f) + Decimal(symbol.l)
        return Ok(None)

    def events_matching(self: Self, data: WsEvent.Res) -> Result[None, Exception]:
        """."""
        if data.X == "FILLED":
            symbol = data.s.replace("USDT", "")
            if symbol in self.book:
                self.book[symbol].last_price = Decimal(data.p)
                # send to db
                # delete old orders
                # place new orders
        return Ok(None)

    def processing_events(self: Self, data: WsEvent.Res) -> Result[None, Exception]:
        """."""
        match data.e:
            case "outboundAccountPosition":
                # change balance
                self.events_balance(data)
            case "executionReport":
                # order has completed
                self.events_matching(data)
        return Ok(None)

    async def listen_events_msg(
        self: Self,
        ws_inst: ClientConnection,
    ) -> Result[None, Exception]:
        """Infinity loop for listen events msgs."""
        async for msg in ws_inst:
            match do(
                Ok(None)
                for value in self.parse_bytes_to_dict(msg)
                for data_dataclass in self.convert_to_dataclass_from_dict(
                    WsEvent.Res,
                    value,
                )
                for _ in self.processing_events(data_dataclass)
            ):
                case Err(exc):
                    return Err(exc)
        return Ok(None)

    async def runtime_events_ws(
        self: Self,
        ws: connect,
    ) -> Result[None, Exception]:
        """Runtime listen websocket all time."""
        async with ws as ws_inst:
            match await do_async(
                Ok(None) for _ in await self.listen_events_msg(ws_inst)
            ):
                case Err(exc):
                    return Err(exc)
        return Ok(None)

    def get_url_for_websocket(self: Self, listen_key: str) -> Result[str, Exception]:
        """."""
        return Ok(f"wss://stream.binance.com:443/ws/{listen_key}")

    def get_websocket(
        self: Self,
        url: str,
    ) -> Result[connect, Exception]:
        """Get connect for working with websocket by url."""
        return Ok(
            connect(
                uri=url,
                max_queue=1024,
            ),
        )

    async def events(self: Self) -> Result[None, Exception]:
        """Monitoring events from websocket.

        Start listen websocket
        """
        reconnect_delay = 1
        max_reconnect_delay = 60
        while True:
            try:
                logger.info("events listen start")
                match await do_async(
                    Ok(None)
                    for object_listen_key in await self.get_sapi_v1_user_data_stream({})
                    for listen_key in self.export_listen_key(object_listen_key)
                    for url_ws in self.get_url_for_websocket(listen_key)
                    for ws in self.get_websocket(url_ws)
                    for _ in self.logger_success(f"Got listen key:{listen_key}")
                    for _ in await self.runtime_events_ws(ws)
                ):
                    case Err(exc):
                        logger.exception(exc)
                        await self.send_telegram_msg(
                            "Drop websocket: see logs",
                        )
            except (
                ConnectionResetError,
                websockets_exceptions.ConnectionClosed,
                TimeoutError,
                websockets_exceptions.WebSocketException,
                socket.gaierror,
                ConnectionRefusedError,
                SSLError,
                OSError,
            ) as exc:
                logger.exception(exc)
                await self.send_telegram_msg("Drop websocket: see logs")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            except Exception as exc:  # noqa: BLE001
                logger.exception(exc)
                await self.send_telegram_msg("Unexpected error in websocket: see logs")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    async def alertest(self: Self) -> Result[None, Exception]:
        """Alert statistic."""
        logger.info("alertest")
        while True:
            await do_async(Ok(None) for _ in await self.send_telegram_msg("binance"))
            await asyncio.sleep(60 * 60)
        return Ok(None)

    def decimal_to_str(self: Self, data: Decimal) -> Result[str, Exception]:
        """Convert Decimal to str."""
        return Ok(str(data))

    def quantize_minus(
        self: Self,
        data: Decimal,
        increment: Decimal,
    ) -> Result[Decimal, Exception]:
        """Quantize to down."""
        return Ok(data.quantize(increment, ROUND_DOWN))

    async def start_up_orders(self: Self) -> Result[None, Exception]:
        """."""
        # wait while matcher and balancer would be ready
        await asyncio.sleep(10)
        for ticket in self.book:
            match await do_async(
                Ok(order_result)
                for quantity in self.quantize_minus(
                    self.book[ticket].balance,
                    self.book[ticket].baseincrement,
                )
                for quantity_str in self.decimal_to_str(quantity)
                for order_result in await self.post_sapi_v1_margin_order(
                    {
                        "symbol": f"{ticket}USDT",
                        "side": "SELL",
                        "type": "MARKET",
                        "quantity": quantity_str,
                        "sideEffectType": "AUTO_BORROW_REPAY",
                    },
                )
            ):
                case Ok(order_result):
                    logger.success(order_result)
                case Err(exc):
                    logger.exception(exc)
        return Ok(None)

        return Ok(None)

    async def infinity_task(self: Self) -> Result[None, Exception]:
        """."""
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self.events()),
                tg.create_task(self.alertest()),
                tg.create_task(self.start_up_orders()),
            ]

        for task in tasks:
            return task.result()

        return Ok(None)


async def main() -> Result[None, Exception]:
    """Collect of major func."""
    bnnc = BNNC()
    match await do_async(
        Ok(None)
        for _ in await bnnc.pre_init()
        for _ in bnnc.logger_success("Pre-init OK!")
        for _ in await bnnc.send_telegram_msg("Settings are OK!")
        for _ in await bnnc.infinity_task()
    ):
        case Ok(None):
            pass
        case Err(exc):
            logger.exception(exc)
            return Err(exc)
    await asyncio.sleep(10000)
    return Ok(None)


if __name__ == "__main__":
    """Main enter."""
    asyncio.run(main())
