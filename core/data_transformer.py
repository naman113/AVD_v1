import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class DataTransformer:
    """
    Handles data transformations on MQTT messages before database insertion.
    """
    
    def __init__(self):
        pass
    
    def apply_transformations(self, 
                            data: Dict[str, Any], 
                            topic: str, 
                            transformations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Apply a list of transformations to the data.
        
        Args:
            data: The message data to transform
            topic: The MQTT topic 
            transformations: List of transformation configurations
            
        Returns:
            Transformed data dictionary
        """
        if not transformations:
            return data
        
        # Work on a copy to avoid modifying original
        result_data = data.copy()
        
        for transformation in transformations:
            try:
                # Check if transformation condition is met
                if self._check_condition(result_data, topic, transformation.get('condition', {})):
                    result_data = self._apply_transformation(result_data, transformation)
                    logger.info(f"[TRANSFORMER] Applied transformation '{transformation.get('name', 'unnamed')}' to topic '{topic}'")
            except Exception as e:
                logger.error(f"[TRANSFORMER] Error applying transformation '{transformation.get('name', 'unnamed')}': {e}")
                # Continue with other transformations even if one fails
        
        return result_data
    
    def _check_condition(self, data: Dict[str, Any], topic: str, condition: Dict[str, Any]) -> bool:
        """
        Check if transformation condition is satisfied.
        
        Args:
            data: The message data
            topic: The MQTT topic
            condition: Condition configuration
            
        Returns:
            True if condition is met, False otherwise
        """
        if not condition:
            return True
        
        # Check topic condition
        if 'topic' in condition:
            if condition['topic'] != topic:
                return False
        
        # Check field conditions
        if 'fields' in condition:
            for field_name, expected_value in condition['fields'].items():
                if field_name not in data or data[field_name] != expected_value:
                    return False
        
        # Check field existence
        if 'has_fields' in condition:
            for field_name in condition['has_fields']:
                if field_name not in data:
                    return False
        
        return True
    
    def _apply_transformation(self, data: Dict[str, Any], transformation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply a single transformation to the data.
        
        Args:
            data: The message data to transform
            transformation: Transformation configuration
            
        Returns:
            Transformed data dictionary
        """
        action = transformation.get('action', {})
        action_type = action.get('type')
        
        if action_type == 'combine_decimal':
            return self._combine_decimal_parts(data, action)
        elif action_type == 'scale_value':
            return self._scale_value(data, action)
        elif action_type == 'rename_field':
            return self._rename_field(data, action)
        elif action_type == 'remove_field':
            return self._remove_field(data, action)
        else:
            logger.warning(f"[TRANSFORMER] Unknown transformation type: {action_type}")
            return data
    
    def _combine_decimal_parts(self, data: Dict[str, Any], action: Dict[str, Any]) -> Dict[str, Any]:
        """
        Combine integer and fractional parts into a single decimal value.
        
        Example:
        P0 = 12345, P1 = 81723 -> P0 = 12345.81723
        
        Args:
            data: The message data
            action: Action configuration with integer_field, fractional_field, target_field
            
        Returns:
            Data with combined decimal value
        """
        integer_field = action.get('integer_field')
        fractional_field = action.get('fractional_field')
        target_field = action.get('target_field')
        remove_fractional = action.get('remove_fractional', False)
        
        if not all([integer_field, fractional_field, target_field]):
            logger.error("[TRANSFORMER] combine_decimal requires integer_field, fractional_field, and target_field")
            return data
        
        if integer_field not in data or fractional_field not in data:
            logger.warning(f"[TRANSFORMER] Missing fields for decimal combination: {integer_field}, {fractional_field}")
            return data
        
        try:
            integer_part = int(data[integer_field])
            fractional_part = int(data[fractional_field])
            
            # Determine the number of decimal places based on fractional part
            fractional_str = str(fractional_part)
            decimal_places = len(fractional_str)
            
            # Combine into decimal value
            combined_value = integer_part + (fractional_part / (10 ** decimal_places))
            
            # Update the data
            result_data = data.copy()
            result_data[target_field] = combined_value
            
            # Remove fractional field if requested
            if remove_fractional and fractional_field in result_data:
                del result_data[fractional_field]
            
            logger.info(f"[TRANSFORMER] Combined decimal: {integer_part} + 0.{fractional_str} = {combined_value}")
            return result_data
            
        except (ValueError, TypeError) as e:
            logger.error(f"[TRANSFORMER] Error combining decimal parts: {e}")
            return data
    
    def _scale_value(self, data: Dict[str, Any], action: Dict[str, Any]) -> Dict[str, Any]:
        """
        Scale a numeric value by a factor.
        
        Args:
            data: The message data
            action: Action configuration with field, scale_factor
            
        Returns:
            Data with scaled value
        """
        field = action.get('field')
        scale_factor = action.get('scale_factor', 1.0)
        
        if not field or field not in data:
            return data
        
        try:
            original_value = float(data[field])
            scaled_value = original_value * scale_factor
            
            result_data = data.copy()
            result_data[field] = scaled_value
            
            logger.info(f"[TRANSFORMER] Scaled {field}: {original_value} * {scale_factor} = {scaled_value}")
            return result_data
            
        except (ValueError, TypeError) as e:
            logger.error(f"[TRANSFORMER] Error scaling value: {e}")
            return data
    
    def _rename_field(self, data: Dict[str, Any], action: Dict[str, Any]) -> Dict[str, Any]:
        """
        Rename a field in the data.
        
        Args:
            data: The message data
            action: Action configuration with from_field, to_field
            
        Returns:
            Data with renamed field
        """
        from_field = action.get('from_field')
        to_field = action.get('to_field')
        
        if not all([from_field, to_field]) or from_field not in data:
            return data
        
        result_data = data.copy()
        result_data[to_field] = result_data.pop(from_field)
        
        logger.info(f"[TRANSFORMER] Renamed field: {from_field} -> {to_field}")
        return result_data
    
    def _remove_field(self, data: Dict[str, Any], action: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove a field from the data.
        
        Args:
            data: The message data
            action: Action configuration with field
            
        Returns:
            Data with field removed
        """
        field = action.get('field')
        
        if not field or field not in data:
            return data
        
        result_data = data.copy()
        del result_data[field]
        
        logger.info(f"[TRANSFORMER] Removed field: {field}")
        return result_data
