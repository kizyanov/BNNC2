"""BNNC2 trading bot for binance."""

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from hashlib import sha256
from hmac import HMAC
from hmac import new as hmac_new
from os import environ
from time import time
from typing import Any, Self
from urllib.parse import urljoin

from aiohttp import ClientConnectorError, ClientSession
from loguru import logger
from orjson import JSONDecodeError, JSONEncodeError, dumps, loads
from result import Err, Ok, Result, do, do_async


@dataclass(frozen=True)
class TelegramSendMsg:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        ok: bool = field(default=False)


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

    def logger_success[T](self: Self, data: T) -> Result[T, Exception]:
        """Success logger for Pipes."""
        logger.success(data)
        return Ok(data)

    def logger_info[T](self: Self, data: T) -> Result[T, Exception]:
        """Info logger for Pipes."""
        logger.info(data)
        return Ok(data)

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

    def get_headers_auth(self: Self) -> Result[dict[str, str], Exception]:
        """Get headers for http request."""
        return Ok(
            {
                "X-MBX-APIKEY": self.KEY,
            },
        )

    def get_http_params(self: Self) -> Result[dict[str, str | int], Exception]:
        """."""
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

    def update_data_signature(
        self: Self,
        data: dict[str, str | int],
        signature: str,
    ) -> Result[dict[str, str | int], Exception]:
        """."""
        data.update({"signature": signature})
        return Ok(data)

    def encrypt_data_params(
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

    async def get_sapi_v1_margin_account(
        self: Self,
    ) -> Result[str, Exception]:
        """Get all account balance.

        https://developers.binance.com/docs/margin_trading/account/Query-Cross-Margin-Account-Details
        """
        uri = "/sapi/v1/margin/account"
        method = "GET"
        return await do_async(
            Ok("test")
            for init_params in self.get_http_params()
            for ff in self.encrypt_data_params(init_params)
            for s in self.update_data_signature(init_params, ff)
            for params_string in self.get_http_params_as_str(s)
            for full_url in self.get_full_url(self.BASE_URL, f"{uri}?" + params_string)
            for headers in self.get_headers_auth()
            for response_bytes in await self.request(
                method=method,
                url=full_url,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for _ in self.logger_info(response_dict)
        )

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
            for _ in await self.get_sapi_v1_margin_account()
        )


async def main() -> Result[None, Exception]:
    """Collect of major func."""
    bnnc = BNNC()
    match await do_async(
        Ok(None)
        for _ in await bnnc.pre_init()
        for _ in bnnc.logger_success("Pre-init OK!")
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
