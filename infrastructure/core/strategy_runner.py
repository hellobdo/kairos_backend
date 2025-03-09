import vectorbt as vbt
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from abc import ABC, abstractmethod
import sqlite3
import importlib
from ..trade_metrics.trade_metrics import TradeMetrics

class StrategyRunner:
    def __init__(self,
                 db_path: str,
                 symbols: List[str],
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 initial_capital: float = 100000,
                 risk_config: Optional[Dict] = None,
                 stoploss_config: Optional[Dict] = None,
                 variable_stoploss_config: Optional[Dict] = None,
                 run_id: Optional[int] = None):
        """
        Runs strategies based on indicator rules.
        
        Args:
            db_path: Path to SQLite database
            symbols: List of symbols to trade
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            initial_capital: Initial capital for backtesting
            risk_config: Risk management configuration
            stoploss_config: Stop loss configuration
            variable_stoploss_config: Variable stop loss configuration (if applicable)
            run_id: ID of the current backtest run
        """
        self.db_path = db_path
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.risk_config = risk_config or {'risk_per_trade': 1.0, 'max_daily_risk': 5.0}
        self.stoploss_config = stoploss_config or {'type': 'fix_perc', 'delta_perc': 1.0}
        self.variable_stoploss_config = variable_stoploss_config
        self.run_id = run_id
        
        # Dictionary to store trade IDs
        self.trade_ids = {}
        
        # Load market data
        self.data = self._load_market_data()
        
    def _calculate_position_size(self, entry_price: float, stop_price: float, entry_time: pd.Timestamp) -> float:
        """
        Calculate position size based on risk parameters and daily risk limits.
        
        Args:
            entry_price: Entry price of the trade
            stop_price: Stop loss price
            entry_time: Timestamp of the entry signal
            
        Returns:
            Number of shares/contracts to trade, or 0 if risk limits exceeded
        """
        print(f"\nCalculating position size for signal at {entry_time}:")
        print(f"Entry Price: ${entry_price:.2f}")
        print(f"Stop Price: ${stop_price:.2f}")
        
        # First check if we have available risk for today
        entry_date = entry_time.date()
        daily_risk_used = self._calculate_daily_risk_used(entry_date)
        max_daily_risk = self.risk_config['max_daily_risk']
        
        print(f"Daily Risk - Used: {daily_risk_used:.2f}%, Max: {max_daily_risk:.2f}%")
        
        # If we've exceeded daily risk limit, skip trade
        if daily_risk_used >= max_daily_risk:
            print(f"Daily risk limit exceeded - Used: {daily_risk_used:.2f}%, Max: {max_daily_risk:.2f}%")
            return 0
            
        # Calculate remaining risk available
        remaining_risk_perc = max_daily_risk - daily_risk_used
        
        # Get configured risk per trade
        risk_per_trade = self.risk_config['risk_per_trade']
        
        # Use the smaller of remaining risk or risk per trade
        risk_to_use = min(remaining_risk_perc, risk_per_trade)
        print(f"Risk to use: {risk_to_use:.2f}% (min of {remaining_risk_perc:.2f}% remaining and {risk_per_trade:.2f}% per trade)")
        
        # Skip if risk is too small
        MIN_RISK_PERC = 0.30
        if risk_to_use < MIN_RISK_PERC:
            print(f"Risk too small - Available: {risk_to_use:.2f}%, Minimum: {MIN_RISK_PERC:.2f}%")
            return 0
            
        # Calculate risk amount in dollars
        risk_amount = self.initial_capital * (risk_to_use / 100.0)
        
        # Calculate risk per share
        risk_per_share = abs(entry_price - stop_price)
        if risk_per_share == 0:
            print(f"Zero risk per share - Entry: ${entry_price:.2f}, Stop: ${stop_price:.2f}")
            return 0
            
        # Calculate position size
        position_size = risk_amount / risk_per_share
        print(f"Position calculated:")
        print(f"- Risk Amount: ${risk_amount:.2f}")
        print(f"- Risk per Share: ${risk_per_share:.2f}")
        print(f"- Position Size: {position_size:.0f} shares")
        print(f"- Position Value: ${(position_size * entry_price):.2f}")
            
        return position_size
        
    def _calculate_daily_risk_used(self, date) -> float:
        """
        Calculate how much risk has been used for a given date from algo_trades.
        For closed trades, uses the actual perc_return.
        For open trades, uses the initial risk_per_trade since we don't know the actual return yet.
        
        Args:
            date: The date to check
            
        Returns:
            Total risk percentage used for the day
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get risk from closed trades (using actual perc_return)
            closed_trades_query = """
                SELECT COALESCE(SUM(ABS(perc_return)), 0) as closed_risk
                FROM algo_trades
                WHERE DATE(entry_timestamp) = ?
                AND run_id = ?
                AND exit_timestamp IS NOT NULL
            """
            
            # Get risk from open trades (using initial risk_per_trade)
            open_trades_query = """
                SELECT COALESCE(SUM(risk_per_trade), 0) as open_risk
                FROM algo_trades
                WHERE DATE(entry_timestamp) = ?
                AND run_id = ?
                AND exit_timestamp IS NULL
            """
            
            # Get closed trades risk
            cursor.execute(closed_trades_query, (date.strftime('%Y-%m-%d'), self.run_id))
            closed_risk = cursor.fetchone()[0] or 0.0
            
            # Get open trades risk
            cursor.execute(open_trades_query, (date.strftime('%Y-%m-%d'), self.run_id))
            open_risk = cursor.fetchone()[0] or 0.0
            
            # Total risk is sum of closed and open trade risks
            total_risk = closed_risk + open_risk
            
            return total_risk
            
        finally:
            conn.close()

    def _calculate_stop_price(self, ohlcv: pd.DataFrame, entries: pd.Series, direction: str) -> pd.Series:
        """
        Calculate stop prices based on configuration.
        
        Args:
            ohlcv: OHLCV data
            entries: Entry signals
            direction: 'long' or 'short'
            
        Returns:
            Series of stop prices
        """
        stops = pd.Series(0.0, index=ohlcv.index)
        entry_prices = ohlcv.loc[entries, 'close']
        
        if self.stoploss_config['type'] == 'fix_abs':
            delta = self.stoploss_config['delta_abs']
            if direction == 'long':
                stops[entries] = entry_prices - delta
            else:
                stops[entries] = entry_prices + delta
                
        elif self.stoploss_config['type'] == 'fix_perc':
            delta = self.stoploss_config['delta_perc'] / 100.0
            if direction == 'long':
                stops[entries] = entry_prices * (1 - delta)
            else:
                stops[entries] = entry_prices * (1 + delta)
                
        elif self.stoploss_config['type'] == 'variable':
            if not self.variable_stoploss_config:
                raise ValueError("Variable stop loss config is required but not provided")
                
            for price_range in self.variable_stoploss_config['price_ranges']:
                # Find entries within this price range
                range_mask = (
                    entries & 
                    (entry_prices >= price_range['min_price']) & 
                    (entry_prices < price_range['max_price'])
                )
                
                if range_mask.any():
                    if price_range.get('delta_abs') is not None:
                        delta = price_range['delta_abs']
                        if direction == 'long':
                            stops[range_mask] = entry_prices[range_mask] - delta
                        else:
                            stops[range_mask] = entry_prices[range_mask] + delta
                    else:
                        delta = price_range['delta_perc'] / 100.0
                        if direction == 'long':
                            stops[range_mask] = entry_prices[range_mask] * (1 - delta)
                        else:
                            stops[range_mask] = entry_prices[range_mask] * (1 + delta)
        
        return stops
    
    def _load_market_data(self) -> Dict[str, pd.DataFrame]:
        """Load market data for all symbols."""
        conn = sqlite3.connect(self.db_path)
        
        data = {}
        date_filter = ""
        params = []
        
        if self.start_date and self.end_date:
            date_filter = "AND date_and_time BETWEEN ? AND ?"
            params.extend([self.start_date, self.end_date])
        
        for symbol in self.symbols:
            query = f"""
            SELECT date_and_time, open, high, low, close, volume
            FROM historical_data_30mins
            WHERE symbol = ?
            {date_filter}
            AND market_session = 'regular'
            ORDER BY date_and_time
            """
            
            symbol_params = [symbol] + params
            df = pd.read_sql_query(query, conn, params=symbol_params)
            df['date_and_time'] = pd.to_datetime(df['date_and_time'])
            df.set_index('date_and_time', inplace=True)
            
            data[symbol] = df
            
        conn.close()
        return data

    def _get_indicator(self, indicator_name: str, params: Dict) -> object:
        """
        Dynamically load and instantiate an indicator.
        
        Args:
            indicator_name: Name of the indicator (e.g., 'TightCandle')
            params: Parameters for the indicator
            
        Returns:
            Instantiated indicator object
        """
        try:
            # Convert CamelCase to snake_case for file name
            module_name = ''.join(['_' + c.lower() if c.isupper() else c 
                                 for c in indicator_name]).lstrip('_')
            
            # Import from indicators module
            module = importlib.import_module(f'infrastructure.indicators.{module_name}')
            indicator_class = getattr(module, indicator_name)
            
            # Filter indicator parameters
            indicator_params = {
                k: v for k, v in params.items()
                if k in ['tightness_threshold', 'wick_ratio_threshold', 'context_bars']
            }
            
            return indicator_class(**indicator_params)
            
        except (ImportError, AttributeError) as e:
            raise ValueError(f"Could not load indicator {indicator_name}: {e}")

    def _check_available_risk(self, date) -> Tuple[bool, float]:
        """
        Check if there is available risk for new trades on this date.
        
        Args:
            date: The date to check
            
        Returns:
            Tuple of (has_available_risk: bool, remaining_risk_perc: float)
        """
        # Get current risk used from algo_trades
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get risk used by all trades for this date
            query = """
                SELECT COALESCE(SUM(ABS(perc_return)), 0) as total_risk
                FROM algo_trades
                WHERE DATE(entry_timestamp) = ?
                AND run_id = ?
            """
            
            cursor.execute(query, (date.strftime('%Y-%m-%d'), self.run_id))
            daily_risk_used = cursor.fetchone()[0] or 0.0
            
            # Calculate remaining risk
            max_daily_risk = self.risk_config['max_daily_risk']
            remaining_risk = max_daily_risk - daily_risk_used
            
            # Check if we have enough risk available (at least minimum required)
            MIN_RISK_PERC = 0.30
            has_available_risk = remaining_risk >= MIN_RISK_PERC
            
            return has_available_risk, remaining_risk
            
        finally:
            conn.close()

    def generate_signals(self, symbol: str, indicator: object, params: Dict) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
        """
        Generate entry and exit signals using the indicator.
        
        Args:
            symbol: Symbol to generate signals for
            indicator: Indicator instance
            params: Strategy parameters
            
        Returns:
            Tuple of (entries, exits, sizes, capital_required, stops) as Series
        """
        # Get market data
        ohlcv = self.data[symbol]
        
        # Get entry signals from indicator
        long_entries, short_entries = indicator.find_entry_signals(ohlcv)
        
        # Initialize position sizes and capital required
        sizes = pd.Series(0.0, index=ohlcv.index)
        stops = pd.Series(0.0, index=ohlcv.index)
        capital_required = pd.Series(0.0, index=ohlcv.index)
        
        # Track days where we've taken trades
        days_with_trades = set()
        
        # Process signals day by day
        for date in pd.date_range(ohlcv.index[0].date(), ohlcv.index[-1].date()):
            # Get signals for this date
            date_mask = ohlcv.index.date == date
            day_data = ohlcv[date_mask]
            day_long_entries = long_entries[date_mask]
            day_short_entries = short_entries[date_mask]
            
            if not (day_long_entries.any() or day_short_entries.any()):
                continue
            
            # Calculate stop prices for this day's signals
            long_stops = self._calculate_stop_price(day_data, day_long_entries, 'long')
            short_stops = self._calculate_stop_price(day_data, day_short_entries, 'short')
            
            # Check if this is the first trade of the day
            is_first_trade = date not in days_with_trades
            
            # Process long entries for this day
            for idx in day_data[day_long_entries].index:
                # Skip daily risk check for first trade of the day
                if not is_first_trade:
                    daily_risk_used = self._calculate_daily_risk_used(date)
                    max_daily_risk = self.risk_config['max_daily_risk']
                    
                    if daily_risk_used >= max_daily_risk:
                        print(f"Skipping rest of day {date} - No available risk. Used: {daily_risk_used}%, Max: {max_daily_risk}%")
                        continue
                    
                size = self._calculate_position_size(
                    day_data.loc[idx, 'close'],
                    long_stops[idx],
                    idx
                )
                if size > 0:
                    sizes[idx] = size
                    stops[idx] = long_stops[idx]
                    capital_required[idx] = self._calculate_capital_required(day_data.loc[idx, 'close'], size)
                    days_with_trades.add(date)  # Mark this day as having trades
                    break  # Take only one trade at a time
                else:
                    print(f"Long signal skipped at {idx} - Price: {day_data.loc[idx, 'close']}, Stop: {long_stops[idx]}")
                
            # If we took a long trade, skip short trades for this bar
            if date in days_with_trades:
                continue
                
            # Process short entries for this day
            for idx in day_data[day_short_entries].index:
                # Skip daily risk check for first trade of the day
                if not is_first_trade:
                    daily_risk_used = self._calculate_daily_risk_used(date)
                    max_daily_risk = self.risk_config['max_daily_risk']
                    
                    if daily_risk_used >= max_daily_risk:
                        print(f"Skipping rest of day {date} - No available risk. Used: {daily_risk_used}%, Max: {max_daily_risk}%")
                        continue
                    
                size = self._calculate_position_size(
                    day_data.loc[idx, 'close'],
                    short_stops[idx],
                    idx
                )
                if size > 0:
                    sizes[idx] = size
                    stops[idx] = short_stops[idx]
                    capital_required[idx] = self._calculate_capital_required(day_data.loc[idx, 'close'], size)
                    days_with_trades.add(date)  # Mark this day as having trades
                    break  # Take only one trade at a time
                else:
                    print(f"Short signal skipped at {idx} - Price: {day_data.loc[idx, 'close']}, Stop: {short_stops[idx]}")
        
        # Only consider entries where we actually took positions
        entries = pd.Series(False, index=ohlcv.index)
        entries[sizes > 0] = True
        
        # Calculate take profit levels for valid entries
        target_risk_reward = params.get('target_risk_reward', 2.0)
        long_tp = pd.Series(0.0, index=ohlcv.index)
        short_tp = pd.Series(0.0, index=ohlcv.index)
        
        # For long trades
        long_mask = entries & long_entries
        if long_mask.any():
            long_risk = ohlcv.loc[long_mask, 'close'] - stops[long_mask]
            long_tp[long_mask] = ohlcv.loc[long_mask, 'close'] + (long_risk * target_risk_reward)
        
        # For short trades
        short_mask = entries & short_entries
        if short_mask.any():
            short_risk = stops[short_mask] - ohlcv.loc[short_mask, 'close']
            short_tp[short_mask] = ohlcv.loc[short_mask, 'close'] - (short_risk * target_risk_reward)
        
        # Generate exit signals
        exits = pd.Series(False, index=ohlcv.index)
        
        # For each entry point where we took a position
        for i, (idx, is_entry) in enumerate(entries.items()):
            if not is_entry:
                continue
                
            # Determine if it's a long or short trade
            is_long = long_entries[idx]
            
            # Get stop and target prices
            stop = stops[idx]
            target = long_tp[idx] if is_long else short_tp[idx]
            
            # Look for exit after entry
            future_prices = ohlcv.iloc[i+1:]
            
            if is_long:
                # Exit when price hits stop loss or take profit
                stop_hit = future_prices['low'] <= stop
                target_hit = future_prices['high'] >= target
            else:
                # Exit when price hits stop loss or take profit
                stop_hit = future_prices['high'] >= stop
                target_hit = future_prices['low'] <= target
            
            # Find first exit point
            exit_idx = None
            if stop_hit.any() and target_hit.any():
                stop_idx = stop_hit.idxmax()
                target_idx = target_hit.idxmax()
                exit_idx = min(stop_idx, target_idx)
            elif stop_hit.any():
                exit_idx = stop_hit.idxmax()
            elif target_hit.any():
                exit_idx = target_hit.idxmax()
            
            if exit_idx:
                exits[exit_idx] = True
        
        return entries, exits, sizes, capital_required, stops

    def backtest(self, symbol: str, strategy_id: int, run_id: int, indicator_name: str, params: Dict) -> vbt.Portfolio:
        """
        Run backtest for a single symbol.
        
        Args:
            symbol: Symbol to backtest
            strategy_id: ID of the strategy from algo_strategies table
            run_id: ID of the backtest run
            indicator_name: Name of the indicator to use
            params: Strategy parameters
            
        Returns:
            VectorBT Portfolio object with backtest results
        """
        # Get indicator instance
        indicator = self._get_indicator(indicator_name, params)
        
        # Generate signals and position sizes
        entries, exits, sizes, capital_required, stops = self.generate_signals(symbol, indicator, params)
        
        # Run backtest
        pf = vbt.Portfolio.from_signals(
            close=self.data[symbol]['close'],
            entries=entries,
            exits=exits,
            size=sizes,  # Use calculated position sizes
            init_cash=self.initial_capital,
            freq='30min'
        )
        
        # Process trades
        if len(pf.trades) > 0:
            for idx, trade in pf.trades.iterrows():
                entry_timestamp = trade.entry_time
                direction = 'long' if trade.size > 0 else 'short'
                
                # Save opening trade data
                opening_trade = {
                    'strategy_id': strategy_id,
                    'symbol': symbol,
                    'entry_timestamp': entry_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'entry_price': trade.entry_price,
                    'stop_price': stops[entry_timestamp],  # From generate_signals
                    'position_size': abs(trade.size),
                    'capital_required': capital_required[entry_timestamp],
                    'direction': direction
                }
                self._save_trade(opening_trade)
                
                # If trade is closed, save closing data
                if trade.status == 'closed':
                    closing_trade = {
                        'symbol': symbol,
                        'entry_timestamp': entry_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        'entry_price': trade.entry_price,
                        'exit_timestamp': trade.exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'exit_price': trade.exit_price,
                        'stop_price': stops[entry_timestamp],
                        'direction': direction
                    }
                    self._save_trade(closing_trade)
        
        return pf

    def _save_trade(self, trade_data: Dict) -> None:
        """
        Save trade information to algo_trades table.
        When a trade is opened, we save the initial trade data.
        When a trade is closed, we update with exit data and performance metrics.
        
        Args:
            trade_data: Dictionary containing trade information
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            if trade_data.get('exit_timestamp') is None:
                # This is a new trade being opened
                query = """
                    INSERT INTO algo_trades (
                        run_id,
                        strategy_id,
                        symbol,
                        entry_timestamp,
                        entry_price,
                        stop_price,
                        position_size,
                        risk_per_trade,
                        capital_required,
                        direction,
                        risk_size
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # Calculate risk size in dollars
                risk_size = abs(trade_data['entry_price'] - trade_data['stop_price']) * trade_data['position_size']
                
                cursor.execute(query, (
                    self.run_id,
                    trade_data['strategy_id'],
                    trade_data['symbol'],
                    trade_data['entry_timestamp'],
                    trade_data['entry_price'],
                    trade_data['stop_price'],
                    trade_data['position_size'],
                    self.risk_config['risk_per_trade'],
                    trade_data['capital_required'],
                    trade_data['direction'],
                    risk_size
                ))
                
                # Get the trade_id and store it
                trade_id = cursor.lastrowid
                trade_key = (
                    trade_data['symbol'],
                    trade_data['entry_timestamp']
                )
                self.trade_ids[trade_key] = trade_id
                
            else:
                # This is updating an existing trade with exit information
                trade_key = (
                    trade_data['symbol'],
                    trade_data['entry_timestamp']
                )
                trade_id = self.trade_ids.get(trade_key)
                
                if trade_id is None:
                    raise ValueError(f"Could not find trade_id for trade: {trade_key}")
                
                # Calculate closing metrics
                closing_metrics = TradeMetrics.calculate_closing_metrics(trade_data)
                
                query = """
                    UPDATE algo_trades
                    SET exit_timestamp = ?,
                        exit_price = ?,
                        winning_trade = ?,
                        trade_duration = ?,
                        perc_return = ?,
                        risk_reward = ?
                    WHERE id = ?
                """
                
                cursor.execute(query, (
                    trade_data['exit_timestamp'],
                    trade_data['exit_price'],
                    closing_metrics['winning_trade'],
                    closing_metrics['trade_duration'],
                    closing_metrics['perc_return'],
                    closing_metrics['risk_reward'],
                    trade_id
                ))
            
            conn.commit()
            
        finally:
            conn.close()

    def _calculate_capital_required(self, entry_price: float, position_size: int) -> float:
        """
        Calculate the capital required for the trade.
        
        Args:
            entry_price: Entry price of the trade
            position_size: Number of shares/contracts
            
        Returns:
            Capital required in dollars
        """
        return float(position_size * entry_price) 