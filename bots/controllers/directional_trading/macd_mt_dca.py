from decimal import Decimal
from typing import List, Optional
import pandas as pd
import pandas_ta as ta  # noqa: F401
from pydantic import Field, validator
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.core.data_type.common import TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.dca_executor.data_types import DCAExecutorConfig, DCAMode
from hummingbot.strategy_v2.models.executor_actions import ExecutorAction, StopExecutorAction


class MacdMTDCAControllerConfig(DirectionalTradingControllerConfigBase):
    controller_name = "macd_mt_dca"



    #general controller config
    min_hold_next_executor: float = Field(
        default=1.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Min to wait to allow creating new executor: ",
            prompt_on_new=True))


    #DCA CONFIG
    dca_spreads: List[Decimal] = Field(
        default="0.01,0.02,0.04,0.08",
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter a comma-separated list of spreads for each DCA level: "))
    dca_amounts: List[Decimal] = Field(
        default="0.1,0.2,0.4,0.8",
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter a comma-separated list of amounts for each DCA level: "))
    time_limit: int = Field(
        default=60 * 60 * 24 * 7, gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the time limit for each DCA level: ",
            prompt_on_new=False))
    stop_loss: Decimal = Field(
        default=Decimal("0.03"), gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the stop loss (as a decimal, e.g., 0.03 for 3%): ",
            prompt_on_new=True))
    executor_refresh_time: Optional[float] = Field(
        default=None,
        client_data=ClientFieldData(
            is_updatable=True,
            prompt_on_new=False))
    executor_activation_bounds: Optional[List[Decimal]] = Field(
        default=None,
        client_data=ClientFieldData(
            is_updatable=True,
            prompt=lambda mi: "Enter the activation bounds for the orders "
                              "(e.g., 0.01 activates the next order when the price is closer than 1%): ",
            prompt_on_new=False))

    @validator("executor_activation_bounds", pre=True, always=True)
    def parse_activation_bounds(cls, v):
        if isinstance(v, list):
            return [Decimal(val) for val in v]
        elif isinstance(v, str):
            if v == "":
                return None
            return [Decimal(val) for val in v.split(",")]
        return v

    @validator('dca_spreads', pre=True, always=True)
    def parse_spreads(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            if v == "":
                return []
            return [float(x.strip()) for x in v.split(',')]
        return v

    @validator('dca_amounts', pre=True, always=True)
    def parse_and_validate_amounts(cls, v, values, field):
        if v is None or v == "":
            return [1 for _ in values[values['dca_spreads']]]
        if isinstance(v, str):
            return [float(x.strip()) for x in v.split(',')]
        elif isinstance(v, list) and len(v) != len(values['dca_spreads']):
            raise ValueError(
                f"The number of {field.name} must match the number of {values['dca_spreads']}.")
        return v

    #candles_data
    candles_config: List[CandlesConfig] = []
    candles_connector: str = Field(
        default=None,
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the connector for the candles data, leave empty to use the same exchange as the connector: ", )
    )
    candles_trading_pair: str = Field(
        default=None,
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the trading pair for the candles data, leave empty to use the same trading pair as the connector: ", )
    )

    #MACDS CONFIG
    macd_interval_1: str = Field(
        default="3m",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the candle interval (e.g., 1m, 5m, 1h, 1d): ",
            prompt_on_new=False))
    macd_fast_1: int = Field(
        default=21,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the MACD fast period: ",
            prompt_on_new=True))
    macd_slow_1: int = Field(
        default=42,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the MACD slow period: ",
            prompt_on_new=True))
    macd_signal_1: int = Field(
        default=9,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the MACD signal period: ",
            prompt_on_new=True))
    macd_signal_type_1: str = Field(
        default="mean_reversion",
        client_data=ClientFieldData(
            prompt=lambda mi: "mean_reversion/trend_following",
            prompt_on_new=False))
    macd_interval_2: str = Field(
        default="3m",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the candle interval (e.g., 1m, 5m, 1h, 1d): ",
            prompt_on_new=False))
    macd_fast_2: int = Field(
        default=21,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the MACD fast period: ",
            prompt_on_new=True))
    macd_slow_2: int = Field(
        default=42,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the MACD slow period: ",
            prompt_on_new=True))
    macd_signal_2: int = Field(
        default=9,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the MACD signal period: ",
            prompt_on_new=True))
    macd_signal_type_2: str = Field(
        default="mean_reversion",
        client_data=ClientFieldData(
            prompt=lambda mi: "mean_reversion/trend_following",
            prompt_on_new=False))

    @validator("candles_connector", pre=True, always=True)
    def set_candles_connector(cls, v, values):
        if v is None or v == "":
            return values.get("connector_name")
        return v

    @validator("candles_trading_pair", pre=True, always=True)
    def set_candles_trading_pair(cls, v, values):
        if v is None or v == "":
            return values.get("trading_pair")
        return v


class BollingerMacdDCAController(DirectionalTradingControllerBase):
    def __init__(self, config: MacdMTDCAControllerConfig, *args, **kwargs):
        self.config = config
        self.dca_amounts_pct = [Decimal(amount) / sum(self.config.dca_amounts) for amount in self.config.dca_amounts]
        self.spreads = self.config.dca_spreads
        max_records_list = self.get_candle_max_records()
        if len(self.config.candles_config) == 0:
            self.config.candles_config = [CandlesConfig(
                connector=config.candles_connector,
                trading_pair=config.candles_trading_pair,
                interval=config.macd_interval_1,
                max_records=max_records_list[0]
            ), CandlesConfig(
                connector=config.candles_connector,
                trading_pair=config.candles_trading_pair,
                interval=config.macd_interval_2,
                max_records=max_records_list[1]
            )]
        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        df_macd_1 = self.market_data_provider.get_candles_df(connector_name=self.config.candles_connector,
                                                      trading_pair=self.config.candles_trading_pair,
                                                      interval=self.config.macd_interval_1,
                                                      max_records=self.config.candles_config[0].max_records)
        # Add indicators
        # macd1
        df_macd_1.ta.macd(fast=self.config.macd_fast_1, slow=self.config.macd_slow_1, signal=self.config.macd_signal_1, append=True)
        macdh = df_macd_1[f"MACDh_{self.config.macd_fast_1}_{self.config.macd_slow_1}_{self.config.macd_signal_1}"]
        macd = df_macd_1[f"MACD_{self.config.macd_fast_1}_{self.config.macd_slow_1}_{self.config.macd_signal_1}"]
        if self.config.macd_signal_type_1 == 'trend_following':
            long_condition = (macdh > 0)
            short_condition = (macdh < 0)
        else:
            long_condition = (macdh > 0) & (macd < 0)
            short_condition = (macdh < 0) & (macd > 0)
        df_macd_1["signal_macd_1"] = 0
        df_macd_1.loc[long_condition, "signal_macd_1"] = 1
        df_macd_1.loc[short_condition, "signal_macd_1"] = -1

        #macd2
        df_macd_2 = self.market_data_provider.get_candles_df(connector_name=self.config.candles_connector,
                                                             trading_pair=self.config.candles_trading_pair,
                                                             interval=self.config.macd_interval_2,
                                                             max_records=self.config.candles_config[1].max_records)
        # Add indicators
        df_macd_2.ta.macd(fast=self.config.macd_fast_2, slow=self.config.macd_slow_2, signal=self.config.macd_signal_2,
                          append=True)
        macdh = df_macd_2[f"MACDh_{self.config.macd_fast_2}_{self.config.macd_slow_2}_{self.config.macd_signal_2}"]
        macd = df_macd_2[f"MACD_{self.config.macd_fast_2}_{self.config.macd_slow_2}_{self.config.macd_signal_2}"]
        if self.config.macd_signal_type_2 == 'trend_following':
            long_condition = (macdh > 0)
            short_condition = (macdh < 0)
        else:
            long_condition = (macdh > 0) & (macd < 0)
            short_condition = (macdh < 0) & (macd > 0)

        df_macd_2["signal_macd_2"] = 0
        df_macd_2.loc[long_condition, "signal_macd_2"] = 1
        df_macd_2.loc[short_condition, "signal_macd_2"] = -1
        # Generate signal
        # Merge DataFrames on timestamp
        df_macd_1['time'] = pd.to_datetime(df_macd_1['timestamp'], unit='ms')
        df_macd_2['time'] = pd.to_datetime(df_macd_2['timestamp'], unit='ms')

        df_merged = pd.merge_asof(df_macd_1[['time', 'signal_macd_1', 'timestamp']],df_macd_2[['time', 'signal_macd_2']],
                                  on='time',
                                  direction='backward',)

        # Compute final signal
        df_merged["signal"] = df_merged.apply(
            lambda row: row['signal_macd_1'] if row['signal_macd_1'] == row['signal_macd_2'] else 0, axis=1)

        # Update processed data
        self.processed_data["signal"] = df_merged["signal"].iloc[-1]
        self.processed_data["features"] = df_merged

    def get_candle_max_records(self):
        #returns list with 2 records: position 0 candle records to macd - position 1 candle records  to bb
        result = []
        interval_durations = {
            '1s': 1,
            '1m': 60,
            '3m': 3 * 60,
            '5m': 5 * 60,
            '15m': 15 * 60,
            '30m': 30 * 60,
            '1h': 60 * 60,
            '4h': 4 * 60 * 60,
            '1d': 24 * 60 * 60,
        }
        total_macd_1_seconds = interval_durations[self.config.macd_interval_1] * (self.config.macd_slow_1 + self.config.macd_signal_1)
        total_macd_2_seconds = interval_durations[self.config.macd_interval_2] * (self.config.macd_slow_2 + self.config.macd_signal_2)
        max_seconds = max(total_macd_1_seconds, total_macd_2_seconds)
        max_records_macd_1 = max_seconds // interval_durations[self.config.macd_interval_1]
        max_records_macd_2 = max_seconds // interval_durations[self.config.macd_interval_2]
        result.append(max_records_macd_1)
        result.append(max_records_macd_2)
        return result

    def order_level_refresh_condition(self, executor):
        return self.market_data_provider.time() - executor.timestamp > self.config.executor_refresh_time * 1000

    def executors_to_refresh(self) -> List[ExecutorAction]:
        executors_to_refresh = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: not x.is_trading and x.is_active and (self.order_level_refresh_condition(x)))
        return [StopExecutorAction(
            controller_id=self.config.id,
            executor_id=executor.id) for executor in executors_to_refresh]

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal ):
        if trade_type == TradeType.BUY:
            prices = [price * (1 - spread) for spread in self.spreads]
        else:
            prices = [price * (1 + spread) for spread in self.spreads]
        amounts = [amount * pct for pct in self.dca_amounts_pct]
        amounts_quote = [amount * price for amount, price in zip(amounts, prices)]
        return DCAExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            mode=DCAMode.TAKER,
            side=trade_type,
            prices=prices,
            amounts_quote=amounts_quote,
            time_limit=self.config.time_limit,
            stop_loss=self.config.stop_loss,
            take_profit=self.config.take_profit,
            trailing_stop=self.config.trailing_stop,
            activation_bounds=self.config.executor_activation_bounds,
            leverage=self.config.leverage,
        )
