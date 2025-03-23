from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset

class BaseStrategy(Strategy):
    """
    Base Strategy class containing common helper methods for trading strategies.
    
    This class provides reusable functionality for:
    - Position limit checking
    - Time condition verification
    - Indicator application
    - Quantity calculation
    - Stop loss determination
    - Price level calculation
    
    Extend this class to create specific trading strategies without duplicating code.
    """
    
    def _load_indicators(self, indicators, load_function):
        """
        Load indicator calculation functions based on indicator names
        
        Args:
            indicators (list): List of indicator names to load
            load_function (function): Function to load each indicator
            
        Returns:
            dict: Dictionary mapping indicator names to their calculation functions
        """
        calculate_indicators = {}
        for indicator in indicators:
            try:
                calculate_indicators[indicator] = load_function(indicator)
            except Exception as e:
                print(f"Error loading indicator '{indicator}': {str(e)}")
                # Continue with other indicators
        return calculate_indicators
    
    def _check_position_limits(self):
        """Check if we've reached position or loss limits"""
        max_loss_positions = self.parameters.get("max_loss_positions")
        open_positions = [p for p in self.get_positions() if not (p.asset.symbol == "USD" and p.asset.asset_type == Asset.AssetType.FOREX)]
        return len(open_positions) >= max_loss_positions or self.vars.daily_loss_count >= max_loss_positions
    
    def _check_time_conditions(self, time):
        """Check if current time meets our trading conditions (0 or 30 minutes past the hour)"""
        return time.minute == 0 or time.minute == 30
    
    def _apply_indicators(self, df, calculate_indicators):
        """Apply all indicators sequentially, return True if all signals are valid, False otherwise"""
        for calc_func in calculate_indicators.values():
            # Apply indicator and check if signal is false
            df = calc_func(df)
            if not df['is_indicator'].iloc[-1]:
                return False, df
        # All indicators returned True
        return True, df
    
    def _calculate_qty_based_on_risk_per_trade(self, stop_loss_amount, risk_per_trade):
        """Calculate quantity based on risk per trade and stop loss amount"""

        risk_size = 30000 * risk_per_trade
        return int(risk_size // stop_loss_amount)
    
    def _determine_stop_loss(self, price, stop_loss_rules):
        """Determine stop loss amount based on price and rules"""

        for rule in stop_loss_rules:
            if "price_below" in rule and price < rule["price_below"]:
                return rule["amount"]
            elif "price_above" in rule and price >= rule["price_above"]:
                return rule["amount"]
        return None  # No matching rule found
    
    def _calculate_price_levels(self, entry_price, stop_loss_amount, side, risk_reward):
        """Calculate stop loss and take profit levels based on entry price and trade side"""
        
        if side == 'buy':
            stop_loss_price = entry_price - stop_loss_amount
            take_profit_price = entry_price + (stop_loss_amount * risk_reward)
        elif side == 'sell':
            stop_loss_price = entry_price + stop_loss_amount
            take_profit_price = entry_price - (stop_loss_amount * risk_reward)
            
        return stop_loss_price, take_profit_price 