"""
Request Validation for Kurd

Validates tool arguments and enforces constraints.

Usage:
    from kurd.request_validation import RequestValidator

    validator = RequestValidator()

    # Add constraint to tool
    validator.add_constraint("add", "a", min_value=0, max_value=1000)
    validator.add_constraint("add", "b", min_value=0, max_value=1000)

    # Validate request
    valid, error = validator.validate("add", {"a": 5, "b": 10})
"""

from typing import Dict, Any, Optional, List, Set
from enum import Enum
import re


class ConstraintType(Enum):
    """Types of constraints."""

    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    PATTERN = "pattern"
    REGEX = "regex"
    ENUM = "enum"
    REQUIRED = "required"
    TYPE = "type"


class RequestValidator:
    """Validates tool requests against constraints."""

    def __init__(self):
        self.constraints: Dict[str, Dict[str, List[tuple]]] = {}
        self.type_constraints: Dict[str, Dict[str, str]] = {}

    def add_constraint(
        self,
        tool_name: str,
        parameter: str,
        constraint_type: str = "min_value",
        value: Any = None,
        **kwargs
    ) -> None:
        """
        Add constraint to a parameter.

        Args:
            tool_name: Name of tool
            parameter: Parameter name
            constraint_type: Type of constraint (min_value, max_value, pattern, etc)
            value: Constraint value
        """
        if tool_name not in self.constraints:
            self.constraints[tool_name] = {}

        if parameter not in self.constraints[tool_name]:
            self.constraints[tool_name][parameter] = []

        self.constraints[tool_name][parameter].append((constraint_type, value, kwargs))

    def set_parameter_type(self, tool_name: str, parameter: str, param_type: str) -> None:
        """Set expected type for parameter."""
        if tool_name not in self.type_constraints:
            self.type_constraints[tool_name] = {}

        self.type_constraints[tool_name][parameter] = param_type

    def validate(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
    ) -> tuple[bool, Optional[str]]:
        """
        Validate tool arguments.

        Returns:
            (is_valid, error_message)
        """
        if tool_name not in self.constraints:
            return True, None

        constraints = self.constraints[tool_name]
        type_constraints = self.type_constraints.get(tool_name, {})

        for param_name, param_constraints in constraints.items():
            if param_name not in arguments:
                # Check if required
                for constraint_type, value, kwargs in param_constraints:
                    if constraint_type == "required" and value is True:
                        return False, f"Parameter '{param_name}' is required"
                continue

            param_value = arguments[param_name]

            # Check type
            if param_name in type_constraints:
                expected_type = type_constraints[param_name]
                actual_type = self._get_type_name(param_value)
                if actual_type != expected_type:
                    return False, f"Parameter '{param_name}' must be {expected_type}, got {actual_type}"

            # Validate constraints
            for constraint_type, constraint_value, kwargs in param_constraints:
                valid, error = self._validate_constraint(
                    param_name,
                    param_value,
                    constraint_type,
                    constraint_value,
                    kwargs,
                )
                if not valid:
                    return False, error

        return True, None

    def _validate_constraint(
        self,
        param_name: str,
        param_value: Any,
        constraint_type: str,
        constraint_value: Any,
        kwargs: Dict,
    ) -> tuple[bool, Optional[str]]:
        """Validate single constraint."""
        try:
            if constraint_type == "min_value":
                if param_value < constraint_value:
                    return False, f"Parameter '{param_name}' must be >= {constraint_value}"

            elif constraint_type == "max_value":
                if param_value > constraint_value:
                    return False, f"Parameter '{param_name}' must be <= {constraint_value}"

            elif constraint_type == "min_length":
                if len(str(param_value)) < constraint_value:
                    return False, f"Parameter '{param_name}' must have length >= {constraint_value}"

            elif constraint_type == "max_length":
                if len(str(param_value)) > constraint_value:
                    return False, f"Parameter '{param_name}' must have length <= {constraint_value}"

            elif constraint_type == "pattern":
                if not str(param_value).startswith(constraint_value):
                    return False, f"Parameter '{param_name}' must start with '{constraint_value}'"

            elif constraint_type == "regex":
                if not re.match(constraint_value, str(param_value)):
                    return False, f"Parameter '{param_name}' must match pattern '{constraint_value}'"

            elif constraint_type == "enum":
                if param_value not in constraint_value:
                    return False, f"Parameter '{param_name}' must be one of: {constraint_value}"

            return True, None
        except Exception as e:
            return False, f"Validation error for '{param_name}': {str(e)}"

    def _get_type_name(self, value: Any) -> str:
        """Get type name of value."""
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "number"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            return type(value).__name__

    def get_constraints(self, tool_name: str) -> Dict[str, List[tuple]]:
        """Get all constraints for a tool."""
        return self.constraints.get(tool_name, {})

    def export_constraints(self) -> Dict:
        """Export all constraints."""
        return {
            "constraints": self.constraints,
            "types": self.type_constraints,
        }

    def import_constraints(self, data: Dict) -> None:
        """Import constraints."""
        self.constraints = data.get("constraints", {})
        self.type_constraints = data.get("types", {})
