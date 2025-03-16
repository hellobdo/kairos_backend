import unittest
import sqlite3
import sys
import os
import logging

# Add the project root to sys.path to allow direct imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the functions to test
from backtests.helpers.get_entry_config import get_entry_config
from backtests.helpers.get_exits_config import get_exits_config
from backtests.helpers.get_risk_config import get_risk_config
from backtests.helpers.get_stoploss_config import get_stoploss_config
from backtests.helpers.get_swing_config import get_swing_config

# Global variables
MODULES_TO_SILENCE = [
    'backtests.helpers.get_entry_config',
    'backtests.helpers.get_exits_config',
    'backtests.helpers.get_risk_config',
    'backtests.helpers.get_stoploss_config',
    'backtests.helpers.get_swing_config'
]

class TestGetEntryConfig(unittest.TestCase):
    """Tests for get_entry_config.py helper function"""
    
    def setUp(self):
        """Set up test environment"""
        # Test database path - this should point to the real database
        self.db_path = "data/algos.db"
        
        # Connect to the database directly
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Store original logging levels to restore later
        self.original_log_levels = {}
        # Suppress logs from the modules we're testing
        for module_name in MODULES_TO_SILENCE:
            logger = logging.getLogger(module_name)
            self.original_log_levels[module_name] = logger.level
            logger.setLevel(logging.CRITICAL)  # Silence all but critical logs
        
    def tearDown(self):
        """Clean up after test"""
        # Close database connection
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            
        # Restore original logging levels
        for module_name, level in self.original_log_levels.items():
            logging.getLogger(module_name).setLevel(level)
    
    def test_get_entry_config(self):
        """Test get_entry_config returns the expected structure and data"""
        # Get all config IDs to test
        self.cursor.execute("SELECT id FROM manager_entry")
        config_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not config_ids:
            self.skipTest("No entry configs found in database")
        
        # Expected fields in the return value
        expected_fields = {'id', 'field', 'signal', 'direction'}
        
        # Ensure we run at least one test
        self.assertGreater(len(config_ids), 0, "No entry configs found to test")    
        tested_count = 0
            
        # Test each config ID
        for config_id in config_ids:
            # Get data directly using SQL for comparison
            self.cursor.execute("SELECT id, field, signal, direction FROM manager_entry WHERE id = ?", (config_id,))
            row = self.cursor.fetchone()
            
            # Get data using the helper function
            result = get_entry_config(config_id, self.db_path)
            
            # Check that result is a dictionary
            self.assertIsInstance(result, dict, f"Result for config ID {config_id} should be a dictionary")
            
            # Check that all expected fields are present
            for field in expected_fields:
                self.assertIn(field, result, f"Expected field '{field}' missing from result for config ID {config_id}")
            
            # Check no unexpected fields
            for field in result:
                self.assertIn(field, expected_fields, f"Unexpected field '{field}' in result for config ID {config_id}")
            
            # Check values of fields from the database
            self.assertEqual(result['id'], row[0], f"id mismatch for config ID {config_id}")
            self.assertEqual(result['field'], row[1], f"field mismatch for config ID {config_id}")
            self.assertEqual(result['signal'], row[2], f"signal mismatch for config ID {config_id}")
            self.assertEqual(result['direction'], row[3], f"direction mismatch for config ID {config_id}")
            
            tested_count += 1
            
        # Ensure at least one config was tested
        self.assertGreater(tested_count, 0, "No entry configurations were actually tested")
            
    def test_nonexistent_config(self):
        """Test getting a config that doesn't exist returns None"""
        # Get highest ID in the table
        self.cursor.execute("SELECT MAX(id) FROM manager_entry")
        max_id = self.cursor.fetchone()[0] or 0
        
        # Try to get a config with an ID that doesn't exist
        nonexistent_id = max_id + 1000
        result = get_entry_config(nonexistent_id, self.db_path)
        
        # Should return None
        self.assertIsNone(result, f"Expected None for nonexistent config ID {nonexistent_id}")


class TestGetExitsConfig(unittest.TestCase):
    """Tests for get_exits_config.py helper function"""
    
    def setUp(self):
        """Set up test environment"""
        # Test database path - this should point to the real database
        self.db_path = "data/algos.db"
        
        # Connect to the database directly
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Store original logging levels to restore later
        self.original_log_levels = {}
        # Suppress logs from the modules we're testing
        for module_name in MODULES_TO_SILENCE:
            logger = logging.getLogger(module_name)
            self.original_log_levels[module_name] = logger.level
            logger.setLevel(logging.CRITICAL)  # Silence all but critical logs
        
    def tearDown(self):
        """Clean up after test"""
        # Close database connection
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            
        # Restore original logging levels
        for module_name, level in self.original_log_levels.items():
            logging.getLogger(module_name).setLevel(level)
    
    def test_get_exits_config_fixed(self):
        """Test get_exits_config returns the expected structure and data for fixed exit types"""
        # Get fixed exit config IDs
        self.cursor.execute("SELECT id FROM manager_exits WHERE type = 'fixed'")
        config_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not config_ids:
            self.skipTest("No fixed exit configs found in database")
            
        # Expected fields in the return value for fixed exit config
        expected_fields = {'id', 'type', 'name', 'description', 'size_exit', 'risk_reward'}
        
        # Ensure we run at least one test
        self.assertGreater(len(config_ids), 0, "No fixed exit configs found to test")
        tested_count = 0
            
        # Test each fixed config ID
        for config_id in config_ids:
            # Get base data directly using SQL for comparison
            self.cursor.execute("SELECT id, type, name, description FROM manager_exits WHERE id = ?", (config_id,))
            base_row = self.cursor.fetchone()
            
            # Get ranges data directly using SQL for comparison
            self.cursor.execute("""
                SELECT size_exit, risk_reward
                FROM manager_exits_ranges
                WHERE exit_id = ?
                ORDER BY size_exit DESC
            """, (config_id,))
            ranges_row = self.cursor.fetchone()
            
            # Skip if no ranges found
            if not ranges_row:
                continue
            
            # Get data using the helper function
            result = get_exits_config(config_id)
            
            # Check that result is a dictionary
            self.assertIsInstance(result, dict, f"Result for fixed exit config ID {config_id} should be a dictionary")
            
            # Check that all expected fields are present
            for field in expected_fields:
                self.assertIn(field, result, f"Expected field '{field}' missing from result for fixed exit ID {config_id}")
            
            # Check no unexpected fields
            for field in result:
                self.assertIn(field, expected_fields, f"Unexpected field '{field}' in result for fixed exit ID {config_id}")
            
            # Check values of fields from the base table
            self.assertEqual(result['id'], base_row[0], f"id mismatch for fixed exit ID {config_id}")
            self.assertEqual(result['type'], base_row[1], f"type mismatch for fixed exit ID {config_id}")
            self.assertEqual(result['name'], base_row[2], f"name mismatch for fixed exit ID {config_id}")
            self.assertEqual(result['description'], base_row[3], f"description mismatch for fixed exit ID {config_id}")
            
            # Check values from the ranges table with appropriate type handling
            if isinstance(ranges_row[0], str) and ranges_row[0] == 'size':
                self.assertEqual(result['size_exit'], 'size', f"size_exit should be 'size' for fixed exit ID {config_id}")
                self.assertIsNone(result['risk_reward'], f"risk_reward should be None for fixed exit ID {config_id}")
            else:
                self.assertEqual(result['size_exit'], float(ranges_row[0]), f"size_exit mismatch for fixed exit ID {config_id}")
                expected_rr = float(ranges_row[1]) if ranges_row[1] is not None else None
                self.assertEqual(result['risk_reward'], expected_rr, f"risk_reward mismatch for fixed exit ID {config_id}")
            
            tested_count += 1
            
        # Ensure at least one config was tested
        self.assertGreater(tested_count, 0, "No fixed exit configurations were actually tested")
    
    def test_get_exits_config_variable(self):
        """Test get_exits_config returns the expected structure and data for variable exit types"""
        # Get variable exit config IDs
        self.cursor.execute("SELECT id FROM manager_exits WHERE type = 'variable'")
        config_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not config_ids:
            self.skipTest("No variable exit configs found in database")
            
        # Expected fields in the return value for variable exit config
        expected_base_fields = {'id', 'type', 'name', 'description', 'ranges'}
        # Expected fields in each range dictionary
        expected_range_fields = {'size_exit', 'risk_reward'}
        
        # Ensure we run at least one test
        self.assertGreater(len(config_ids), 0, "No variable exit configs found to test")
        tested_count = 0
            
        # Test each variable config ID
        for config_id in config_ids:
            # Get base data directly using SQL for comparison
            self.cursor.execute("SELECT id, type, name, description FROM manager_exits WHERE id = ?", (config_id,))
            base_row = self.cursor.fetchone()
            
            # Get ranges data directly using SQL for comparison
            self.cursor.execute("""
                SELECT size_exit, risk_reward
                FROM manager_exits_ranges
                WHERE exit_id = ?
                ORDER BY size_exit DESC
            """, (config_id,))
            ranges_rows = self.cursor.fetchall()
            
            # Skip if no ranges found
            if not ranges_rows:
                continue
            
            # Get data using the helper function
            result = get_exits_config(config_id)
            
            # Check that result is a dictionary
            self.assertIsInstance(result, dict, f"Result for variable exit ID {config_id} should be a dictionary")
            
            # Check that all expected base fields are present
            for field in expected_base_fields:
                self.assertIn(field, result, f"Expected field '{field}' missing from result for variable exit ID {config_id}")
            
            # Check no unexpected base fields
            for field in result:
                self.assertIn(field, expected_base_fields, f"Unexpected field '{field}' in result for variable exit ID {config_id}")
            
            # Check values of fields from the base table
            self.assertEqual(result['id'], base_row[0], f"id mismatch for variable exit ID {config_id}")
            self.assertEqual(result['type'], base_row[1], f"type mismatch for variable exit ID {config_id}")
            self.assertEqual(result['name'], base_row[2], f"name mismatch for variable exit ID {config_id}")
            self.assertEqual(result['description'], base_row[3], f"description mismatch for variable exit ID {config_id}")
            
            # Check that ranges is a list with the right length
            self.assertIsInstance(result['ranges'], list, f"ranges should be a list for variable exit ID {config_id}")
            self.assertEqual(len(result['ranges']), len(ranges_rows), 
                            f"ranges list length mismatch for variable exit ID {config_id}")
            
            # Check each range
            for i, (range_dict, db_row) in enumerate(zip(result['ranges'], ranges_rows)):
                # Check structure of range dictionary
                self.assertIsInstance(range_dict, dict, f"Range {i} should be a dictionary for variable exit ID {config_id}")
                
                # Check expected fields in range
                for field in expected_range_fields:
                    self.assertIn(field, range_dict, f"Expected field '{field}' missing from range {i} for variable exit ID {config_id}")
                
                # Check no unexpected fields in range
                for field in range_dict:
                    self.assertIn(field, expected_range_fields, f"Unexpected field '{field}' in range {i} for variable exit ID {config_id}")
                
                # Check values in range
                if isinstance(db_row[0], str) and db_row[0] == 'size':
                    self.assertEqual(range_dict['size_exit'], 'size', f"size_exit should be 'size' in range {i} for variable exit ID {config_id}")
                    self.assertIsNone(range_dict['risk_reward'], f"risk_reward should be None in range {i} for variable exit ID {config_id}")
                else:
                    self.assertEqual(range_dict['size_exit'], float(db_row[0]), 
                                    f"size_exit mismatch in range {i} for variable exit ID {config_id}")
                    expected_rr = float(db_row[1]) if db_row[1] is not None else None
                    self.assertEqual(range_dict['risk_reward'], expected_rr, 
                                    f"risk_reward mismatch in range {i} for variable exit ID {config_id}")
            
            tested_count += 1
            
        # Ensure at least one config was tested
        self.assertGreater(tested_count, 0, "No variable exit configurations were actually tested")
    
    def test_nonexistent_exits_config(self):
        """Test getting an exits config that doesn't exist returns None"""
        # Get highest ID in the table
        self.cursor.execute("SELECT MAX(id) FROM manager_exits")
        max_id = self.cursor.fetchone()[0] or 0
        
        # Try to get a config with an ID that doesn't exist
        nonexistent_id = max_id + 1000
        result = get_exits_config(nonexistent_id)
        
        # Should return None
        self.assertIsNone(result, f"Expected None for nonexistent exits config ID {nonexistent_id}")


class TestGetRiskConfig(unittest.TestCase):
    """Tests for get_risk_config.py helper function"""
    
    def setUp(self):
        """Set up test environment"""
        # Test database path - this should point to the real database
        self.db_path = "data/algos.db"
        
        # Connect to the database directly
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Store original logging levels to restore later
        self.original_log_levels = {}
        # Suppress logs from the modules we're testing
        for module_name in MODULES_TO_SILENCE:
            logger = logging.getLogger(module_name)
            self.original_log_levels[module_name] = logger.level
            logger.setLevel(logging.CRITICAL)  # Silence all but critical logs
        
    def tearDown(self):
        """Clean up after test"""
        # Close database connection
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            
        # Restore original logging levels
        for module_name, level in self.original_log_levels.items():
            logging.getLogger(module_name).setLevel(level)
    
    def test_get_risk_config(self):
        """Test get_risk_config returns the expected structure and data"""
        # Get all risk config IDs to test
        self.cursor.execute("SELECT id FROM manager_risk")
        config_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not config_ids:
            self.skipTest("No risk configs found in database")
        
        # Expected fields in the return value
        expected_fields = {'id', 'risk_per_trade', 'max_daily_risk', 'outside_regular_hours_allowed'}
        
        # Ensure we run at least one test
        self.assertGreater(len(config_ids), 0, "No risk configs found to test")    
        tested_count = 0
            
        # Test each config ID
        for config_id in config_ids:
            # Get data directly using SQL for comparison
            self.cursor.execute("""
                SELECT id, risk_per_trade, max_daily_risk, outside_regular_hours_allowed 
                FROM manager_risk 
                WHERE id = ?
            """, (config_id,))
            row = self.cursor.fetchone()
            
            # Get data using the helper function
            result = get_risk_config(config_id)
            
            # Check that result is a dictionary
            self.assertIsInstance(result, dict, f"Result for risk config ID {config_id} should be a dictionary")
            
            # Check that all expected fields are present
            for field in expected_fields:
                self.assertIn(field, result, f"Expected field '{field}' missing from result for risk config ID {config_id}")
            
            # Check no unexpected fields
            for field in result:
                self.assertIn(field, expected_fields, f"Unexpected field '{field}' in result for risk config ID {config_id}")
            
            # Check values of fields from the database with appropriate conversions
            self.assertEqual(result['id'], row[0], f"id mismatch for risk config ID {config_id}")
            self.assertEqual(result['risk_per_trade'], row[1]/100, f"risk_per_trade mismatch for risk config ID {config_id}")
            self.assertEqual(result['max_daily_risk'], row[2]/100, f"max_daily_risk mismatch for risk config ID {config_id}")
            self.assertEqual(result['outside_regular_hours_allowed'], row[3], 
                             f"outside_regular_hours_allowed mismatch for risk config ID {config_id}")
            
            tested_count += 1
            
        # Ensure at least one config was tested
        self.assertGreater(tested_count, 0, "No risk configurations were actually tested")
            
    def test_nonexistent_risk_config(self):
        """Test getting a risk config that doesn't exist returns None"""
        # Get highest ID in the table
        self.cursor.execute("SELECT MAX(id) FROM manager_risk")
        max_id = self.cursor.fetchone()[0] or 0
        
        # Try to get a config with an ID that doesn't exist
        nonexistent_id = max_id + 1000
        result = get_risk_config(nonexistent_id)
        
        # Should return None
        self.assertIsNone(result, f"Expected None for nonexistent risk config ID {nonexistent_id}")


class TestGetStoplossConfig(unittest.TestCase):
    """Tests for get_stoploss_config.py helper function"""
    
    def setUp(self):
        """Set up test environment"""
        # Test database path - this should point to the real database
        self.db_path = "data/algos.db"
        
        # Connect to the database directly
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Store original logging levels to restore later
        self.original_log_levels = {}
        # Suppress logs from the modules we're testing
        for module_name in MODULES_TO_SILENCE:
            logger = logging.getLogger(module_name)
            self.original_log_levels[module_name] = logger.level
            logger.setLevel(logging.CRITICAL)  # Silence all but critical logs
        
    def tearDown(self):
        """Clean up after test"""
        # Close database connection
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            
        # Restore original logging levels
        for module_name, level in self.original_log_levels.items():
            logging.getLogger(module_name).setLevel(level)
    
    def test_get_stoploss_config_fixed_abs(self):
        """Test get_stoploss_config returns the expected structure and data for fixed absolute stop types"""
        # Get fixed absolute stoploss config IDs
        self.cursor.execute("SELECT id FROM manager_stoploss WHERE type = 'fix_abs'")
        config_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not config_ids:
            self.skipTest("No fixed absolute stoploss configs found in database")
        
        # Expected fields in the return value
        expected_fields = {'stop_type', 'stop_value', 'name', 'description'}
        
        # Ensure we run at least one test
        self.assertGreater(len(config_ids), 0, "No fixed absolute stoploss configs found to test")    
        tested_count = 0
            
        # Test each config ID
        for config_id in config_ids:
            # Get data directly using SQL for comparison
            self.cursor.execute("""
                SELECT delta_abs, name, description 
                FROM manager_stoploss 
                WHERE id = ?
            """, (config_id,))
            row = self.cursor.fetchone()
            
            # Get data using the helper function
            result = get_stoploss_config(config_id)
            
            # Check that result is a dictionary
            self.assertIsInstance(result, dict, f"Result for stoploss config ID {config_id} should be a dictionary")
            
            # Check that all expected fields are present
            for field in expected_fields:
                self.assertIn(field, result, f"Expected field '{field}' missing from result for stoploss config ID {config_id}")
            
            # Check no unexpected fields
            for field in result:
                self.assertIn(field, expected_fields, f"Unexpected field '{field}' in result for stoploss config ID {config_id}")
            
            # Check values and type conversions
            self.assertEqual(result['stop_type'], 'abs', f"stop_type should be 'abs' for fixed absolute stoploss ID {config_id}")
            self.assertEqual(result['stop_value'], row[0], f"stop_value mismatch for stoploss config ID {config_id}")
            self.assertEqual(result['name'], row[1], f"name mismatch for stoploss config ID {config_id}")
            self.assertEqual(result['description'], row[2], f"description mismatch for stoploss config ID {config_id}")
            
            tested_count += 1
            
        # Ensure at least one config was tested
        self.assertGreater(tested_count, 0, "No fixed absolute stoploss configurations were actually tested")
    
    def test_get_stoploss_config_fixed_perc(self):
        """Test get_stoploss_config returns the expected structure and data for fixed percentage stop types"""
        # Get fixed percentage stoploss config IDs
        self.cursor.execute("SELECT id FROM manager_stoploss WHERE type = 'fix_perc'")
        config_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not config_ids:
            self.skipTest("No fixed percentage stoploss configs found in database")
        
        # Expected fields in the return value
        expected_fields = {'stop_type', 'stop_value', 'name', 'description'}
        
        # Ensure we run at least one test
        self.assertGreater(len(config_ids), 0, "No fixed percentage stoploss configs found to test")    
        tested_count = 0
            
        # Test each config ID
        for config_id in config_ids:
            # Get data directly using SQL for comparison
            self.cursor.execute("""
                SELECT delta_perc, name, description 
                FROM manager_stoploss 
                WHERE id = ?
            """, (config_id,))
            row = self.cursor.fetchone()
            
            # Get data using the helper function
            result = get_stoploss_config(config_id)
            
            # Check that result is a dictionary
            self.assertIsInstance(result, dict, f"Result for stoploss config ID {config_id} should be a dictionary")
            
            # Check that all expected fields are present
            for field in expected_fields:
                self.assertIn(field, result, f"Expected field '{field}' missing from result for stoploss config ID {config_id}")
            
            # Check no unexpected fields
            for field in result:
                self.assertIn(field, expected_fields, f"Unexpected field '{field}' in result for stoploss config ID {config_id}")
            
            # Check values and type conversions
            self.assertEqual(result['stop_type'], 'perc', f"stop_type should be 'perc' for fixed percentage stoploss ID {config_id}")
            self.assertEqual(result['stop_value'], row[0]/100.0, f"stop_value mismatch for stoploss config ID {config_id}")
            self.assertEqual(result['name'], row[1], f"name mismatch for stoploss config ID {config_id}")
            self.assertEqual(result['description'], row[2], f"description mismatch for stoploss config ID {config_id}")
            
            tested_count += 1
            
        # Ensure at least one config was tested
        self.assertGreater(tested_count, 0, "No fixed percentage stoploss configurations were actually tested")
    
    def test_get_stoploss_config_variable(self):
        """Test get_stoploss_config returns the expected structure and data for variable stop types"""
        # Get variable stoploss config IDs
        self.cursor.execute("SELECT id FROM manager_stoploss WHERE type = 'variable'")
        config_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not config_ids:
            self.skipTest("No variable stoploss configs found in database")
        
        # Expected fields in the return value
        expected_fields = {'stop_type', 'stop_func', 'name', 'description'}
        
        # Ensure we run at least one test
        self.assertGreater(len(config_ids), 0, "No variable stoploss configs found to test")    
        tested_count = 0
            
        # Test each config ID
        for config_id in config_ids:
            # Get data directly using SQL for comparison
            self.cursor.execute("""
                SELECT name, description 
                FROM manager_stoploss 
                WHERE id = ?
            """, (config_id,))
            base_row = self.cursor.fetchone()
            
            # Get ranges data for comparison - using correct table and column names
            # Now include delta_abs field
            self.cursor.execute("""
                SELECT min_price, max_price, delta_perc, delta_abs
                FROM manager_stoploss_price_ranges
                WHERE style_id = ?
                ORDER BY min_price
            """, (config_id,))
            ranges_rows = self.cursor.fetchall()
            
            # Skip if no ranges found
            if not ranges_rows:
                continue
            
            # Get data using the helper function
            result = get_stoploss_config(config_id)
            
            # Check that result is a dictionary
            self.assertIsInstance(result, dict, f"Result for variable stoploss config ID {config_id} should be a dictionary")
            
            # Check that all expected fields are present
            for field in expected_fields:
                self.assertIn(field, result, f"Expected field '{field}' missing from result for variable stoploss config ID {config_id}")
            
            # Check no unexpected fields
            for field in result:
                self.assertIn(field, expected_fields, f"Unexpected field '{field}' in result for variable stoploss config ID {config_id}")
            
            # Check values and type conversions
            self.assertEqual(result['stop_type'], 'custom', f"stop_type should be 'custom' for variable stoploss ID {config_id}")
            self.assertEqual(result['name'], base_row[0], f"name mismatch for variable stoploss config ID {config_id}")
            self.assertEqual(result['description'], base_row[1], f"description mismatch for variable stoploss config ID {config_id}")
            
            # Check that stop_func is callable
            self.assertTrue(callable(result['stop_func']), f"stop_func should be callable for variable stoploss ID {config_id}")
            
            # Test stop_func with specific test cases to verify both delta_perc and delta_abs are handled correctly
            for min_price, max_price, delta_perc, delta_abs in ranges_rows:
                if min_price is not None and max_price is not None:
                    # Test a price in the middle of the range
                    test_price = (min_price + max_price) / 2
                    actual_stop = result['stop_func'](test_price)
                    
                    # Verify the stop value is calculated correctly based on available data
                    self.assertIsInstance(actual_stop, (int, float), 
                                 f"stop_func should return a number for price {test_price}")
                    
                    # Test that the function uses delta_perc if available, otherwise delta_abs
                    if delta_perc is not None:
                        expected_stop = delta_perc / 100.0
                        self.assertAlmostEqual(actual_stop, expected_stop, places=4,
                                              msg=f"When delta_perc is available, stop_func should return delta_perc/100 for price {test_price}")
                    elif delta_abs is not None and test_price != 0:
                        expected_stop = delta_abs / test_price
                        self.assertAlmostEqual(actual_stop, expected_stop, places=4,
                                              msg=f"When only delta_abs is available, stop_func should return delta_abs/price for price {test_price}")
            
            tested_count += 1
            
        # Ensure at least one config was tested
        self.assertGreater(tested_count, 0, "No variable stoploss configurations were actually tested")
            
    def test_nonexistent_stoploss_config(self):
        """Test getting a stoploss config that doesn't exist returns None"""
        # Get highest ID in the table
        self.cursor.execute("SELECT MAX(id) FROM manager_stoploss")
        max_id = self.cursor.fetchone()[0] or 0
        
        # Try to get a config with an ID that doesn't exist
        nonexistent_id = max_id + 1000
        result = get_stoploss_config(nonexistent_id)
        
        # Should return None
        self.assertIsNone(result, f"Expected None for nonexistent stoploss config ID {nonexistent_id}")


class TestGetSwingConfig(unittest.TestCase):
    """Tests for get_swing_config.py helper function"""
    
    def setUp(self):
        """Set up test environment"""
        # Test database path - this should point to the real database
        self.db_path = "data/algos.db"
        
        # Connect to the database directly
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Store original logging levels to restore later
        self.original_log_levels = {}
        # Suppress logs from the modules we're testing
        for module_name in MODULES_TO_SILENCE:
            logger = logging.getLogger(module_name)
            self.original_log_levels[module_name] = logger.level
            logger.setLevel(logging.CRITICAL)  # Silence all but critical logs
        
    def tearDown(self):
        """Clean up after test"""
        # Close database connection
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            
        # Restore original logging levels
        for module_name, level in self.original_log_levels.items():
            logging.getLogger(module_name).setLevel(level)
    
    def test_get_swing_config(self):
        """Test get_swing_config returns the expected structure and data"""
        # Get all swing config IDs to test
        self.cursor.execute("SELECT id FROM manager_swings")
        config_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not config_ids:
            self.skipTest("No swing configs found in database")
        
        # Expected fields in the return value
        expected_fields = {'id', 'swings_allowed', 'description'}
        
        # Ensure we run at least one test
        self.assertGreater(len(config_ids), 0, "No swing configs found to test")    
        tested_count = 0
            
        # Test each config ID
        for config_id in config_ids:
            # Get data directly using SQL for comparison
            self.cursor.execute("""
                SELECT id, swings_allowed, description 
                FROM manager_swings 
                WHERE id = ?
            """, (config_id,))
            row = self.cursor.fetchone()
            
            # Get data using the helper function
            result = get_swing_config(config_id)
            
            # Check that result is a dictionary
            self.assertIsInstance(result, dict, f"Result for swing config ID {config_id} should be a dictionary")
            
            # Check that all expected fields are present
            for field in expected_fields:
                self.assertIn(field, result, f"Expected field '{field}' missing from result for swing config ID {config_id}")
            
            # Check no unexpected fields
            for field in result:
                self.assertIn(field, expected_fields, f"Unexpected field '{field}' in result for swing config ID {config_id}")
            
            # Check values of fields from the database
            self.assertEqual(result['id'], row[0], f"id mismatch for swing config ID {config_id}")
            self.assertEqual(result['swings_allowed'], row[1], f"swings_allowed mismatch for swing config ID {config_id}")
            self.assertEqual(result['description'], row[2], f"description mismatch for swing config ID {config_id}")
            
            tested_count += 1
            
        # Ensure at least one config was tested
        self.assertGreater(tested_count, 0, "No swing configurations were actually tested")
            
    def test_nonexistent_swing_config(self):
        """Test getting a swing config that doesn't exist returns None"""
        # Get highest ID in the table
        self.cursor.execute("SELECT MAX(id) FROM manager_swings")
        max_id = self.cursor.fetchone()[0] or 0
        
        # Try to get a config with an ID that doesn't exist
        nonexistent_id = max_id + 1000
        result = get_swing_config(nonexistent_id)
        
        # Should return None
        self.assertIsNone(result, f"Expected None for nonexistent swing config ID {nonexistent_id}")


if __name__ == "__main__":
    # Use a simpler approach - just run the tests and print results
    print("Running tests for config helper functions...")
    
    # Suppress unittest output
    result = unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    
    # Print custom summary with checkmarks
    print("\n=== Test Results ===")
    tests_run = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped) if hasattr(result, 'skipped') else 0
    
    print(f"Total tests: {tests_run}")
    print(f"Passed: {tests_run - failures - errors - skipped} ✓")
    
    # Print failures and errors with X marks
    if failures > 0:
        print(f"Failed: {failures} ✗")
        for i, failure in enumerate(result.failures, 1):
            print(f"  ✗ {i}. {failure[0]}")
    
    if errors > 0:
        print(f"Errors: {errors} ✗")
        for i, error in enumerate(result.errors, 1):
            print(f"  ✗ {i}. {error[0]}")
            
    if skipped > 0:
        print(f"Skipped: {skipped} ⚠")
        
    # Indicate overall success/failure
    if failures == 0 and errors == 0:
        print("\n✓ All tests passed!")
    else:
        print("\n✗ Some tests failed.")
        
    sys.exit(not result.wasSuccessful()) 