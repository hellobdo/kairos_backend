import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, Optional, Dict, Any, Union, List
from utils.db_utils import DatabaseManager

db = DatabaseManager()

class TradeProcessor:
    """
    Class for processing trade execution data and aggregating it into trade summaries
    """
    def __init__(self, executions_df: pd.DataFrame):
        """
        Initialize with execution data
        
        Parameters:
            executions_df: DataFrame containing execution data
        """
        self.executions_df = executions_df
        self.entry_execs = None
        self.exit_execs = None
        self.trade_directions = {}
        
    def validate(self) -> bool:
        """
        Validate the executions DataFrame has all required columns
        
        Returns:
            True if valid, False otherwise
        """
        if self.executions_df.empty:
            print("Executions DataFrame is empty")
            return False
            
        required_cols = ['trade_id', 'symbol', 'date', 'time_of_day', 'is_entry', 
                         'is_exit', 'quantity', 'execution_timestamp', 'price']
        missing_cols = [col for col in required_cols if col not in self.executions_df.columns]
        if missing_cols:
            print(f"Missing required columns: {missing_cols}")
            return False
            
        return True
    
    def preprocess(self) -> bool:
        """
        Preprocess the executions data by filtering entries/exits and analyzing trade directions
        
        Returns:
            True if preprocessing succeeded, False otherwise
        """
        # Filter entry and exit executions
        self.entry_execs = self.executions_df[self.executions_df['is_entry'] == 1]
        self.exit_execs = self.executions_df[self.executions_df['is_exit'] == 1]
        
        if self.entry_execs.empty:
            print("No entry executions found")
            return False
            
        # Analyze trade directions
        return self._analyze_trade_directions()
        
    def _analyze_trade_directions(self) -> bool:
        """
        Analyze and store direction data for all trades based on the initial entry execution
        
        Returns:
            True if analysis succeeded, False otherwise
        """
        try:
            # Handle empty entry_execs case
            if self.entry_execs.empty:
                return True

            # Process each trade's entry execution
            for trade_id, group in self.entry_execs.groupby('trade_id'):
                try:
                    # Get the initial entry execution's quantity to determine direction
                    # We look at the first entry execution (by timestamp) to determine direction
                    entry_execution = group.sort_values('execution_timestamp').iloc[0]
                    initial_quantity = entry_execution['quantity']
                    
                    # Determine direction based on the INITIAL entry quantity
                    if initial_quantity > 0:
                        direction = 'bullish'
                    elif initial_quantity < 0:
                        direction = 'bearish'
                    else:
                        print(f"Trade {trade_id} has zero quantity entry which is invalid")
                        return False
                        
                    # Store results for this trade
                    # Convert numpy numeric types to Python native types
                    self.trade_directions[trade_id] = {
                        'direction': direction,
                        'initial_quantity': float(initial_quantity),
                        'abs_initial_quantity': float(abs(initial_quantity))
                    }
                except (KeyError, IndexError) as e:
                    print(f"Error analyzing trade {trade_id}: {str(e)}")
                    return False
            
            return True
        except Exception as e:
            print(f"Error in _analyze_trade_directions: {str(e)}")
            return False
    
    def process_trades(self) -> Optional[pd.DataFrame]:
        """
        Generate a trades DataFrame from preprocessed execution data
        
        Returns:
            DataFrame with trade information or None if processing fails
        """
        try:
            # Validate and preprocess data
            if not self.validate() or not self.preprocess():
                return None
                
            # Get all aggregated data
            aggs = self._get_all_aggregations()
            
            # Build the final DataFrame
            return self._build_trades_dataframe(aggs)
                
        except Exception as e:
            print(f"Unexpected error processing trades: {str(e)}")
            return None
            
    def _calculate_risk_reward_ratio(self, entry_prices: pd.Series = None, exit_prices: pd.Series = None, stop_prices: pd.Series = None) -> pd.Series:
        """
        Calculate risk-reward ratio for each trade
        
        For bullish trades: (exit_price - entry_price) / (entry_price - stop_price)
        For bearish trades: (entry_price - exit_price) / (stop_price - entry_price)
        
        Parameters:
            stop_loss_amount: The amount to use for stop loss calculation
            entry_prices: Series with entry prices indexed by trade_id
            exit_prices: Series with exit prices indexed by trade_id
            stop_prices: Series with stop prices indexed by trade_id
            
        Returns:
            Series with risk-reward ratios indexed by trade_id
        """
        risk_reward_ratios = {}
        
        # Process each trade
        for trade_id, info in self.trade_directions.items():
            entry_price = entry_prices.get(trade_id)
            exit_price = exit_prices.get(trade_id)
            stop_price = stop_prices.get(trade_id)
            direction = info['direction']
            
            # Skip if any price is missing
            if entry_price is None or exit_price is None or stop_price is None:
                risk_reward_ratios[trade_id] = None
                continue

            
            if direction == 'bullish':
                risk = entry_price - stop_price
                reward = exit_price - entry_price
            else:
                risk = stop_price - entry_price
                reward = entry_price - exit_price
            
            # Calculate R:R ratio, handling division by zero
            if risk <= 0:
                risk_reward_ratios[trade_id] = None
            else:
                risk_reward_ratios[trade_id] = reward / risk
        
        return pd.Series(risk_reward_ratios)

    def _get_winning_trades(self, risk_reward: pd.Series) -> pd.Series:
        """
        Determine if each trade was a winner based on risk-reward ratio
        
        A trade is considered a winner if its risk-reward ratio is greater than 0,
        which means the trade made money regardless of the direction.
        
        Parameters:
            risk_reward: Series with risk-reward ratios indexed by trade_id
            
        Returns:
            Series with 1 for winning trades and 0 for losing trades, indexed by trade_id
        """
 
        # A trade is a winner if risk-reward > 0
        winners = {}
        for trade_id, rr in risk_reward.items():
            winners[trade_id] = 1 if (rr is not None and rr > 0) else 0
            
        return pd.Series(winners)

    def _calculate_perc_return(self, risk_per_trade: pd.Series, risk_reward: pd.Series) -> pd.Series:
        """
        Calculate the percentage return for each trade
        
        Percentage return is calculated as risk_per_trade * risk_reward, which gives
        the actual percentage gain/loss of the trade relative to the account balance.
        
        Parameters:
            risk_per_trade: Series with risk per trade values indexed by trade_id
            risk_reward: Series with risk-reward ratios indexed by trade_id
            
        Returns:
            Series with percentage returns indexed by trade_id
        """
        
        # Calculate percentage return
        perc_return = {}
        for trade_id in self.trade_directions.keys():
            rpt = risk_per_trade.get(trade_id)
            rr = risk_reward.get(trade_id)
            
            # Skip if either value is None
            if rpt is None or rr is None:
                perc_return[trade_id] = None
            else:
                # Percentage return = risk per trade * risk reward
                perc_return[trade_id] = rpt * rr
        
        return pd.Series(perc_return)

    def _get_trade_status(self) -> pd.Series:
        """
        Determine the status of each trade (open or closed)
        
        A trade is considered 'open' if:
        - Direction is bullish AND sum of all quantities > 0 (still holding long position)
        - Direction is bearish AND sum of all quantities < 0 (still holding short position)
        
        Otherwise, the trade is considered 'closed'.
        
        Returns:
            Series with 'open' or 'closed' status for each trade_id
        """
        trade_status = {}
        
        # Calculate net position (sum of all quantities) for each trade
        net_positions = self.executions_df.groupby('trade_id')['quantity'].sum()
        
        for trade_id, info in self.trade_directions.items():
            direction = info['direction']
            
            # Get net position (sum of all quantities) for this trade
            net_position = net_positions.get(trade_id)

            if net_position is None:
                raise ValueError(f"Trade {trade_id} has no net position")
            
            # Determine status based on direction and net position
            if net_position == 0:
                trade_status[trade_id] = 'closed'
            elif (direction == 'bullish' and net_position > 0) or (direction == 'bearish' and net_position < 0):
                # Still holding position in the trade direction
                trade_status[trade_id] = 'open'
            else:
                raise ValueError(f"Trade {trade_id} has an invalid net position: {net_position}")
        
        return pd.Series(trade_status)

    def _get_exit_type(self, risk_reward: pd.Series, risk_reward_goal: float, status: pd.Series) -> pd.Series:
        """
        Determine the exit type for each trade based on risk-reward ratio
        
        Exit types:
        - 'stop_loss': Trade was stopped out (risk_reward <= -1)
        - 'take_profit': Trade hit take profit target (risk_reward >= risk_reward_goal)
        - 'other': Trade was exited for other reasons
        
        Parameters:
            risk_reward: Series with risk-reward ratios indexed by trade_id
            risk_reward_goal: Target risk-reward ratio
            status: Series with trade status indexed by trade_id
            
        Returns:
            Series with exit types indexed by trade_id
        """
        
        # Determine exit type
        exit_types = {}
        for trade_id, rr in risk_reward.items():
            # If trade is still open, exit type is None
            if status.get(trade_id) == 'open':
                exit_types[trade_id] = None
                continue
                
            # Skip if risk-reward is None
            # Use pandas.isna() which handles None, NaN, and other NA values
            if pd.isna(rr):
                exit_types[trade_id] = None
                continue
                
            # Determine exit type based on risk-reward
            if rr <= -1:
                exit_types[trade_id] = 'stop_loss'
            # Handle None risk_reward_goal by treating it as infinity (no take_profit classification)
            elif risk_reward_goal is not None and rr >= risk_reward_goal:
                exit_types[trade_id] = 'take_profit'
            else:
                exit_types[trade_id] = 'other'
        
        return pd.Series(exit_types)

    def _get_all_aggregations(self) -> Dict[str, Union[pd.Series, pd.DataFrame]]:
        """
        Get all aggregated data for trades
        
        Returns:
            Dictionary of Series with aggregated data
        """
        try:
            # Create direction Series for backwards compatibility
            direction_series = pd.Series({k: v['direction'] for k, v in self.trade_directions.items()})
            
            # 1. Get number of executions
            num_executions = self._get_num_executions()
            
            # 2. Get symbols
            symbols = self._get_symbols()
            
            # 3. Get entry date/time information
            entry_info = self._get_entry_date_time_info()
            
            # 4. Get quantity, entry price, and capital required in one call
            quantity, entry_price, capital_required = self._get_quantity_and_entry_price()
            
            # 5. Get exit price
            exit_price = self._get_exit_price()
            
            # 6. Define stop loss amount (used by multiple methods)
            stop_loss_amount = 0.02

            # 7. Get stop price 
            stop_price = self._get_stop_prices(
                stop_loss_amount=stop_loss_amount, 
                entry_prices=entry_price
            )
            
            # 8. Calculate risk-reward ratio
            risk_reward = self._calculate_risk_reward_ratio(
                entry_prices=entry_price,
                exit_prices=exit_price,
                stop_prices=stop_price
            )
            
            # 9. Get risk amount per share
            risk_amount_per_share = self._get_risk_amount_per_share(
                entry_prices=entry_price, 
                stop_prices=stop_price
            )
            
            # 10. Get risk per trade
            risk_per_trade = self._get_risk_per_trade(
                risk_per_trade=None,
                risk_amount_per_share=risk_amount_per_share,
                quantity=quantity,
                entry_info=entry_info
            )
            
            # 11. Calculate percentage return
            perc_return = self._calculate_perc_return(
                risk_per_trade=risk_per_trade,
                risk_reward=risk_reward
            )
            
            # 12. Get winning trades 
            is_winner = self._get_winning_trades(
                risk_reward=risk_reward
            )
            
            # 13. Get trade status
            status = self._get_trade_status()
            
            # 14. Set default risk-reward goal and get take profit price
            risk_reward_goal = None
            try:
                take_profit_price = self._get_take_profit_price(
                    risk_reward_goal=risk_reward_goal,
                    entry_prices=entry_price,
                    risk_amount_per_share=risk_amount_per_share
                )
            except Exception as e:
                print(f"Error calculating take profit prices: {str(e)}")
                take_profit_price = pd.Series()
            
            # 15. Get exit type
            try:
                exit_type = self._get_exit_type(
                    risk_reward=risk_reward,
                    risk_reward_goal=risk_reward_goal,
                    status=status
                )
            except Exception as e:
                print(f"Error determining exit types: {str(e)}")
                exit_type = pd.Series()
            
            # 16. Get end date and time
            end_date, end_time = self._get_end_date_and_time()
            
            # 17. Get duration hours
            duration_hours = self._get_duration_hours()
            
            # 18. Get commission
            commission = self._get_commission()
            
            # Return all aggregations
            return {
                'num_executions': num_executions,
                'symbol': symbols,
                'direction': direction_series,
                'quantity': quantity,
                'entry_price': entry_price,
                'capital_required': capital_required,
                'exit_price': exit_price,
                'stop_price': stop_price,
                'take_profit_price': take_profit_price,
                'risk_reward': risk_reward,
                'risk_amount_per_share': risk_amount_per_share,
                'is_winner': is_winner,
                'risk_per_trade': risk_per_trade,
                'perc_return': perc_return,
                'status': status,
                'exit_type': exit_type,
                'end_date': end_date,
                'end_time': end_time,
                'duration_hours': duration_hours,
                'commission': commission,
                'start_date': entry_info['start_date'],
                'start_time': entry_info['start_time'],
                'week': entry_info['week'],
                'month': entry_info['month'],
                'year': entry_info['year']
            }
        except Exception as e:
            print(f"Error in _get_all_aggregations: {str(e)}")
            return {}
        
    def _build_trades_dataframe(self, aggs: Dict[str, Union[pd.Series, pd.DataFrame]]) -> pd.DataFrame:
        """
        Build the final trades DataFrame from aggregations
        
        Parameters:
            aggs: Dictionary of Series with aggregated data
            
        Returns:
            DataFrame with trade information
        """
        # Create base DataFrame with trade_ids and num_executions
        num_executions = aggs.pop('num_executions')
        trades_df = num_executions.copy()
        
        # Add all columns in one pass
        for col_name, series in aggs.items():
            trades_df[col_name] = trades_df['trade_id'].map(series)
            
        return trades_df
        
    def _get_num_executions(self) -> pd.DataFrame:
        """Get the number of executions per trade_id"""
        return self.executions_df.groupby('trade_id').size().rename('num_executions').reset_index()
        
    def _get_entry_date_time_info(self) -> pd.DataFrame:
        """Get comprehensive entry information"""
        # If no entries, return empty DataFrame
        if self.entry_execs.empty:
            return pd.DataFrame()
            
        # Sort by execution_timestamp to ensure chronological order
        sorted_entries = self.entry_execs.sort_values('execution_timestamp')
        
        # Group by trade_id and get first values for date and time
        start_dates = sorted_entries.groupby('trade_id')['date'].first()
        start_times = sorted_entries.groupby('trade_id')['time_of_day'].first()
        
        # Convert string dates to datetime objects
        date_objects = pd.to_datetime(start_dates)
        
        # Create a DataFrame with all entry information
        return pd.DataFrame({
            'start_date': start_dates,
            'start_time': start_times,
            'week': date_objects.dt.isocalendar().week,
            'month': date_objects.dt.month,
            'year': date_objects.dt.year
        })
    
    def _get_end_date_and_time(self) -> Tuple[pd.Series, pd.Series]:
        """
        Get the end date and time for each trade_id based on exit executions
        
        Returns:
            Tuple containing (end_date_series, end_time_series)
        """
        # If no exits, return empty Series with appropriate indexes
        if self.exit_execs.empty:
            return pd.Series(index=self.entry_execs['trade_id']), pd.Series(index=self.entry_execs['trade_id'])
        
        # Sort by execution_timestamp to ensure chronological order
        sorted_exits = self.exit_execs.sort_values('execution_timestamp')
        
        # Group by trade_id and get last values for date and time
        end_dates = sorted_exits.groupby('trade_id')['date'].last()
        end_times = sorted_exits.groupby('trade_id')['time_of_day'].last()
        
        return end_dates, end_times
    
    def _get_duration_hours(self) -> pd.Series:
        """Get the duration in hours for each trade_id"""
        # Get entry timestamps
        entry_times = self.entry_execs.groupby('trade_id')['execution_timestamp'].first()
        
        # If no exits, return Series with NaN values
        if self.exit_execs.empty:
            return pd.Series(index=entry_times.index)
        
        # Get exit timestamps
        exit_times = self.exit_execs.groupby('trade_id')['execution_timestamp'].last()
        
        # Calculate durations only for trades with both entry and exit
        durations = {}
        for trade_id in entry_times.index:
            if trade_id in exit_times.index:
                # Check if exit time is earlier than or equal to entry time
                if exit_times[trade_id] < entry_times[trade_id]:
                    raise ValueError(f"Exit time for trade {trade_id} is earlier than or equal to entry time")
                durations[trade_id] = (exit_times[trade_id] - entry_times[trade_id]).total_seconds() / 3600
        
        return pd.Series(durations)
        
    def _get_symbols(self) -> pd.Series:
        """Get the symbol for each trade_id"""
        return self.executions_df.groupby('trade_id')['symbol'].first()
        
    def _get_direction_executions(self, trade_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Get entry and exit executions for a trade based on its direction
        
        This filters executions based on the trade direction:
        - For bullish trades: 
            - entries are executions with positive quantities
            - exits are executions with negative quantities
        - For bearish trades: 
            - entries are executions with negative quantities
            - exits are executions with positive quantities
        
        Parameters:
            trade_id: Trade identifier
            
        Returns:
            Tuple containing (entry_executions, exit_executions)
        """
        info = self.trade_directions[trade_id]
        is_bullish = info['direction'] == 'bullish'
        
        # Get all executions for this trade
        trade_execs = self.executions_df[self.executions_df['trade_id'] == trade_id]
        
        # Filter executions by direction
        if is_bullish:
            # For bullish trades, entries are buys (positive quantities)
            entry_executions = trade_execs[trade_execs['quantity'] > 0]
            # Exits are sells (negative quantities)
            exit_executions = trade_execs[trade_execs['quantity'] < 0]
        else:
            # For bearish trades, entries are shorts (negative quantities)
            entry_executions = trade_execs[trade_execs['quantity'] < 0]
            # Exits are covers (positive quantities)
            exit_executions = trade_execs[trade_execs['quantity'] > 0]
            
        return entry_executions, exit_executions
        
    def _calculate_vwap(self, executions: pd.DataFrame) -> Optional[float]:
        """
        Calculate volume-weighted average price for a set of executions
        
        Parameters:
            executions: DataFrame containing execution data with price and quantity columns
            
        Returns:
            VWAP as a float or None if executions are empty
        """
        return None if executions.empty or executions['quantity'].abs().sum() == 0 else (executions['price'] * executions['quantity'].abs()).sum() / executions['quantity'].abs().sum()
        
    def _get_quantity_and_entry_price(self) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Get the total quantity, entry price, and capital required for each trade_id
        
        This calculates these metrics together for efficiency.
        Capital required is calculated as quantity * entry_price.
        
        Returns:
            Tuple containing (quantity_series, entry_price_series, capital_required_series)
        """
        trade_quantities = {}
        entry_prices = {}
        capital_required = {}
        
        # Process each trade
        for trade_id in self.trade_directions:
            # Get direction-matched executions
            entry_executions, _ = self._get_direction_executions(trade_id)
            
            # Calculate quantity based on entry executions only
            if entry_executions.empty:
                trade_quantities[trade_id] = 0
                entry_prices[trade_id] = None
                capital_required[trade_id] = 0
                continue
                
            abs_quantities = entry_executions['quantity'].abs()
            total_qty = abs_quantities.sum()
            trade_quantities[trade_id] = total_qty
            
            # Calculate entry price (VWAP) using entry executions only
            entry_price = self._calculate_vwap(entry_executions)
            entry_prices[trade_id] = entry_price
            
            # Calculate capital required
            capital_required[trade_id] = total_qty * entry_price if entry_price is not None else 0
        
        return pd.Series(trade_quantities), pd.Series(entry_prices), pd.Series(capital_required)
        
    def _get_exit_price(self) -> pd.Series:
        """
        Get the volume-weighted average exit price for each trade_id
        
        This calculates the VWAP for exit executions for each trade
        
        Returns:
            Series with exit prices indexed by trade_id
        """
        exit_prices = {}
        
        # Process each trade
        for trade_id in self.trade_directions:
            # Get exit executions
            _, exit_executions = self._get_direction_executions(trade_id)
            
            # Calculate exit price (VWAP)
            exit_prices[trade_id] = self._calculate_vwap(exit_executions)
        
        return pd.Series(exit_prices)
        
    def _get_commission(self) -> pd.Series:
        """Get the commission for each trade_id"""
        if 'commission' in self.executions_df.columns:
            return self.executions_df.groupby('trade_id')['commission'].sum()
        return pd.Series(index=self.trade_directions.keys())

    def _get_stop_prices(self, stop_loss_amount, entry_prices: pd.Series) -> pd.Series:
        """
        Calculate stop prices for all trades based on entry price and direction
        
        Parameters:
            stop_loss_amount: The amount to use for stop loss calculation
            entry_prices: Series with entry prices indexed by trade_id
            
        Returns:
            Series with stop prices indexed by trade_id
        """
        stop_prices = {}
        
        # Check if stop_loss_amount is valid
        valid_stop_loss = stop_loss_amount > 0
        
        # Calculate stop price for each trade
        for trade_id, info in self.trade_directions.items():
            direction = info['direction']
            entry_price = entry_prices.get(trade_id)
            
            # Group all conditions that result in None
            if not valid_stop_loss or entry_price is None or (direction == 'bullish' and stop_loss_amount >= entry_price):
                stop_prices[trade_id] = None
                continue
                
            # Calculate stop price based on direction
            if direction == 'bullish':
                # For bullish trades, stop is below entry
                stop_prices[trade_id] = entry_price - stop_loss_amount
            else:
                # For bearish trades, stop is above entry
                stop_prices[trade_id] = entry_price + stop_loss_amount
        
        return pd.Series(stop_prices)

    def _get_risk_per_trade(self, risk_per_trade: Optional[float], risk_amount_per_share: pd.Series, quantity: pd.Series, entry_info: pd.DataFrame) -> pd.Series:
        """
        Calculate the risk per trade as a percentage of account balance
        
        Risk per trade is calculated as:
        risk_amount_per_share * quantity / account_balance
        
        Parameters:
            risk_per_trade: Optional fixed percentage to use for all trades (overrides calculations)
            risk_amount_per_share: Series with risk amount per share indexed by trade_id
            quantity: Series with quantity values indexed by trade_id
            entry_info: DataFrame with entry information including start_date
            
        Returns:
            Series with risk percentage per trade indexed by trade_id
        """
        # If a fixed risk_per_trade value is provided, use it for all trades
        if risk_per_trade is not None:
            return pd.Series({trade_id: risk_per_trade for trade_id in self.trade_directions.keys()})
            
        try:
            # Get account balances
            account_balances = db.get_account_balances()
        
            # Initialize dictionary to store risk values for each trade
            risk_per_trade_dict = {}
            
            # Process each trade to calculate its risk
            for trade_id in self.trade_directions.keys():
                # Get risk amount per share and quantity
                risk_per_share = risk_amount_per_share.get(trade_id)
                qty = quantity.get(trade_id)
                
                # Skip if any required value is missing
                if risk_per_share is None or qty is None:
                    risk_per_trade_dict[trade_id] = None
                    continue
                
                # Get the trade's start date
                start_date = entry_info['start_date'].get(trade_id)
                if start_date is None:
                    risk_per_trade_dict[trade_id] = None
                    continue
                    
                # Find the account balance on the date of the trade entry
                matching_balances = account_balances[account_balances['date'] == start_date]
                
                if matching_balances.empty:
                    # Use default risk if no matching balance found
                    risk_per_trade_dict[trade_id] = None
                else:
                    # Calculate risk as a percentage of account balance
                    balance = matching_balances['cash_balance'].iloc[0]
                    risk_amount = risk_per_share * qty
                    risk_per_trade_dict[trade_id] = risk_amount / balance
            
            return pd.Series(risk_per_trade_dict)
            
        except Exception as e:
            print(f"Error calculating risk per trade: {str(e)}")
            # Return None for all trades if calculation fails
            return pd.Series({trade_id: None for trade_id in self.trade_directions.keys()})

    def _get_risk_amount_per_share(self, entry_prices: pd.Series, stop_prices: pd.Series) -> pd.Series:
        """
        Calculate the risk amount per share for each trade
        
        Risk amount per share is the absolute difference between entry price and stop price,
        representing the dollar amount at risk per share regardless of trade direction.
        
        Parameters:
            entry_prices: Series with entry prices indexed by trade_id
            stop_prices: Series with stop prices indexed by trade_id
        
        Returns:
            Series with risk amount per share indexed by trade_id
        """
        
        # Calculate risk amount per share
        risk_amount_per_share = {}
        for trade_id in self.trade_directions.keys():
            entry_price = entry_prices.get(trade_id)
            stop_price = stop_prices.get(trade_id)
            
            # Skip if any price is missing
            if entry_price is None or stop_price is None:
                risk_amount_per_share[trade_id] = None
                continue
            
            # Calculate absolute difference
            risk_amount_per_share[trade_id] = abs(entry_price - stop_price)
        
        return pd.Series(risk_amount_per_share)

    def _get_take_profit_price(self, risk_reward_goal: Optional[float], entry_prices: pd.Series, risk_amount_per_share: pd.Series) -> pd.Series:
        """
        Calculate the take profit price for each trade based on a target risk-reward ratio
        
        For bullish trades: entry_price + (risk_amount_per_share * risk_reward_goal)
        For bearish trades: entry_price - (risk_amount_per_share * risk_reward_goal)
        
        Parameters:
            risk_reward_goal: Target risk-reward ratio (e.g., 2.0 means aiming for 2:1 reward to risk)
                              If None, returns None for all trades
            entry_prices: Series with entry prices indexed by trade_id
            risk_amount_per_share: Series with risk amount per share indexed by trade_id
                              
        Returns:
            Series with take profit prices indexed by trade_id
        """
        # If no risk_reward_goal provided, return None for all trades
        if risk_reward_goal is None:
            return pd.Series({trade_id: None for trade_id in self.trade_directions.keys()})
        
        # Calculate take profit prices
        take_profit_prices = {}
        for trade_id, info in self.trade_directions.items():
            direction = info['direction']
            entry_price = entry_prices.get(trade_id)
            risk_per_share = risk_amount_per_share.get(trade_id)
            
            # Skip if any value is missing
            if entry_price is None or risk_per_share is None or direction is None:
                take_profit_prices[trade_id] = None
                continue
                
            # Calculate take profit price based on direction
            if direction == 'bullish':
                # For bullish trades, take profit is above entry
                take_profit_prices[trade_id] = entry_price + (risk_per_share * risk_reward_goal)
            elif direction == 'bearish':
                # For bearish trades, take profit is below entry
                take_profit_prices[trade_id] = entry_price - (risk_per_share * risk_reward_goal)
            else:
                raise ValueError(f"Invalid take profit price for trade {trade_id}: {direction}")
        
        return pd.Series(take_profit_prices)

def process_trades(executions_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Generates a trades DataFrame from the executions data
    
    This is a wrapper function for the TradeProcessor class that maintains
    backward compatibility with the original function interface.
    
    Parameters:
        executions_df: DataFrame containing execution data
        
    Returns:
        DataFrame with trade information aggregated from executions or None if processing fails
    """
    try:
        processor = TradeProcessor(executions_df)
        return processor.process_trades()
    except Exception as e:
        print(f"Error processing trades: {str(e)}")
        return None