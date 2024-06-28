import os
import time
from collections import defaultdict
from decimal import Decimal
from typing import Dict, List, Optional, Set
from pydantic import Field
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class GenericV2StrategyWithCashOutConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    markets: Dict[str, Set[str]] = {}
    time_to_cash_out: Optional[int] = None
    max_portfolio_loss: float = Field(default=30, client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "RSI lower bound to enter long position (e.g. 30)"))


class GenericV2StrategyWithCashOut(StrategyV2Base):

    initial_portfolio_value = {}
    max_portfolio_value = {}
    cash_out_time = None
    cashing_out = False
    """
    This script runs a generic strategy with cash out feature. Will also check if the controllers configs have been
    updated and apply the new settings.
    The cash out of the script can be set by the time_to_cash_out parameter in the config file. If set, the script will
    stop the controllers after the specified time has passed, and wait until the active executors finalize their
    execution.
    The controllers will also have a parameter to manually cash out. In that scenario, the main strategy will stop the
    specific controller and wait until the active executors finalize their execution. The rest of the executors will
    wait until the main strategy stops them.
    """
    def _init_(self, connectors: Dict[str, ConnectorBase], config: GenericV2StrategyWithCashOutConfig):
        res = super()._init_(connectors, config)
        self.config = config
        self.cashing_out = False
        self.closed_executors_buffer: int = 20
        self.max_portfolio_value = False
        self.initial_portfolio_value = None
        return  res

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        self._last_timestamp = timestamp
        self.apply_initial_setting()

    def get_quote_asset(self, trading_pair):
        return trading_pair.split('-')[-1]

    def _get_current_portfolio_value(self):
        current_port = defaultdict(Decimal)
        pnl_dict = self._get_pnl_by_quote_asset()
        for asset, value in self.initial_portfolio_value.items():
            current_port['asset'] = value + pnl_dict[asset]
        return current_port

    def _get_pnl_by_quote_asset(self):
        pnl_dict = defaultdict(Decimal)
        for controller_id, value in self.controllers.items():
            performance_report = self.executor_orchestrator.generate_performance_report(controller_id)
            pnl_dict[self.get_quote_asset(value.config.trading_pair)] += performance_report.global_pnl_quote
        return pnl_dict

    def _set_cashout_time(self):
        if self.config.time_to_cash_out:
            self.cash_out_time = self.config.time_to_cash_out + time.time()
        else:
            self.cash_out_time = None

    def _set_initial_portfolio_value(self):
        balances = self.connectors['binance_perpetual']._account_balances
        initial_portfolio = defaultdict(Decimal)
        for mkt, trading_pair in self.markets.items():
            quote_asset = self.get_quote_asset(list(trading_pair)[0])
            initial_portfolio[quote_asset] = balances.get(quote_asset, Decimal('0'))
        self.initial_portfolio_value = initial_portfolio

    def update_max_portfolio_value(self):
        current_portfolio = self._get_current_portfolio_value()
        if not self.max_portfolio_value:
            self.max_portfolio_value = current_portfolio
        else:
            for asset, value in self.max_portfolio_value.items():
                if value < current_portfolio[asset]:
                    self.max_portfolio_value[asset] = current_portfolio[asset]
        return False

    def stop_by_portfolio_loss(self):
        for controller_id, controller in self.controllers.items():
            if controller.status == RunnableStatus.RUNNING:
                self.logger().info(f"Controller stopped because maximum asset lost reached {controller_id}.")
                controller.stop()
                executors_to_stop = self.get_executors_by_controller(controller_id)
                self.executor_orchestrator.execute_actions(
                    [StopExecutorAction(executor_id=executor.id,
                                        controller_id=executor.controller_id) for executor in executors_to_stop])

    def control_portfolio_loss(self):
        self.update_max_portfolio_value()
        current_portfolio = self._get_current_portfolio_value()
        for asset, value in current_portfolio.items():
            max_portfolio_asset = self.max_portfolio_value[asset]
            if max_portfolio_asset - value >= self.config.max_portfolio_loss:
                self.stop_by_portfolio_loss()
                break

    def on_tick(self):
        super().on_tick()
        if self.config.max_portfolio_loss:
            self.control_portfolio_loss()
        self.control_cash_out()

    def control_cash_out(self):
        self.evaluate_cash_out_time()
        if self.cashing_out:
            self.check_executors_status()
        else:
            self.check_manual_cash_out()

    def evaluate_cash_out_time(self):
        if self.cash_out_time and self.current_timestamp >= self.cash_out_time and not self.cashing_out:
            self.logger().info("Cash out time reached. Stopping the controllers.")
            for controller_id, controller in self.controllers.items():
                if controller.status == RunnableStatus.RUNNING:
                    self.logger().info(f"Cash out for controller {controller_id}.")
                    controller.stop()
            self.cashing_out = True

    def check_manual_cash_out(self):
        for controller_id, controller in self.controllers.items():
            if controller.config.manual_kill_switch and controller.status == RunnableStatus.RUNNING:
                self.logger().info(f"Manual cash out for controller {controller_id}.")
                controller.stop()
                executors_to_stop = self.get_executors_by_controller(controller_id)
                self.executor_orchestrator.execute_actions(
                    [StopExecutorAction(executor_id=executor.id,
                                        controller_id=executor.controller_id) for executor in executors_to_stop])
            if not controller.config.manual_kill_switch and controller.status == RunnableStatus.TERMINATED:
                self.logger().info(f"Restarting controller {controller_id}.")
                controller.start()

    def check_executors_status(self):
        active_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda executor: executor.status == RunnableStatus.RUNNING
        )
        if not active_executors:
            self.logger().info("All executors have finalized their execution. Stopping the strategy.")
            HummingbotApplication.main_application().stop()
        else:
            non_trading_executors = self.filter_executors(
                executors=active_executors,
                filter_func=lambda executor: not executor.is_trading
            )
            self.executor_orchestrator.execute_actions(
                [StopExecutorAction(executor_id=executor.id,
                                    controller_id=executor.controller_id) for executor in non_trading_executors])

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def apply_initial_setting(self):
        connectors_position_mode = {}
        for controller_id, controller in self.controllers.items():
            config_dict = controller.config.dict()
            if "connector_name" in config_dict:
                if self.is_perpetual(config_dict["connector_name"]):
                    if "position_mode" in config_dict:
                        connectors_position_mode[config_dict["connector_name"]] = config_dict["position_mode"]
                    if "leverage" in config_dict:
                        self.connectors[config_dict["connector_name"]].set_leverage(leverage=config_dict["leverage"],
                                                                                    trading_pair=config_dict["trading_pair"])
        for connector_name, position_mode in connectors_position_mode.items():
            self.connectors[connector_name].set_position_mode(position_mode)
        self._set_cashout_time()
        self._set_initial_portfolio_value()
