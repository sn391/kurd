"""
Billing & Usage Tracking for Kurd

Tracks tool usage per tenant and generates billing records.

Usage:
    from kurd.billing import BillingManager, PricingModel

    billing = BillingManager()

    # Define pricing
    pricing = {
        "add": {"per_call": 0.001, "per_latency_ms": 0.0001},
        "multiply": {"per_call": 0.002, "per_latency_ms": 0.0001},
    }
    billing.set_pricing(pricing)

    # Track usage
    billing.track_call(
        tenant_id="customer-1",
        tool_name="add",
        latency_ms=25.5,
        success=True,
    )

    # Generate billing report
    report = billing.get_usage_report("customer-1", period="2026-08")
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class BillingModel(Enum):
    """Billing models."""

    PER_REQUEST = "per_request"  # Charge per tool call
    PER_LATENCY = "per_latency"  # Charge based on execution time
    TIERED = "tiered"  # Tiered pricing (volume discounts)
    HYBRID = "hybrid"  # Combination of per-request and latency


@dataclass
class ToolPricing:
    """Pricing for a single tool."""

    tool_name: str
    model: BillingModel = BillingModel.PER_REQUEST
    per_call: float = 0.0  # Cost per call
    per_latency_ms: float = 0.0  # Cost per ms of latency
    per_token: float = 0.0  # Cost per token (for LLM tools)
    minimum_charge: float = 0.0
    maximum_charge: Optional[float] = None
    free_tier_calls: int = 0  # Free calls per period


@dataclass
class UsageRecord:
    """Single usage record."""

    tenant_id: str
    tool_name: str
    timestamp: datetime
    latency_ms: float
    tokens_used: int = 0
    success: bool = True
    error: Optional[str] = None
    cost: float = 0.0


@dataclass
class TenantQuota:
    """Usage quota for a tenant."""

    tenant_id: str
    max_calls_per_period: Optional[int] = None
    max_cost_per_period: Optional[float] = None
    call_count: int = field(default=0)
    cost_accumulated: float = field(default=0.0)
    period_start: datetime = field(default_factory=datetime.utcnow)


class BillingManager:
    """Manages billing and usage tracking."""

    def __init__(self):
        self.pricing: Dict[str, ToolPricing] = {}
        self.usage_records: List[UsageRecord] = []
        self.tenant_quotas: Dict[str, TenantQuota] = {}
        self.billing_periods: Dict[str, str] = {}  # tenant_id -> period

    def set_pricing(self, pricing: Dict[str, Dict[str, Any]]) -> None:
        """Set pricing for tools."""
        for tool_name, price_config in pricing.items():
            model = BillingModel(price_config.get("model", "per_request"))
            self.pricing[tool_name] = ToolPricing(
                tool_name=tool_name,
                model=model,
                per_call=price_config.get("per_call", 0.0),
                per_latency_ms=price_config.get("per_latency_ms", 0.0),
                per_token=price_config.get("per_token", 0.0),
                minimum_charge=price_config.get("minimum_charge", 0.0),
                maximum_charge=price_config.get("maximum_charge"),
                free_tier_calls=price_config.get("free_tier_calls", 0),
            )

    def set_tenant_quota(
        self,
        tenant_id: str,
        max_calls_per_period: Optional[int] = None,
        max_cost_per_period: Optional[float] = None,
    ) -> None:
        """Set usage quota for tenant."""
        self.tenant_quotas[tenant_id] = TenantQuota(
            tenant_id=tenant_id,
            max_calls_per_period=max_calls_per_period,
            max_cost_per_period=max_cost_per_period,
        )

    def track_call(
        self,
        tenant_id: str,
        tool_name: str,
        latency_ms: float,
        tokens_used: int = 0,
        success: bool = True,
        error: Optional[str] = None,
    ) -> bool:
        """
        Track a tool call for billing.

        Returns:
            True if call was tracked, False if quota exceeded
        """
        # Calculate cost
        cost = self._calculate_cost(tool_name, latency_ms, tokens_used)

        # Check quota
        if not self._check_quota(tenant_id, cost):
            return False

        # Create record
        record = UsageRecord(
            tenant_id=tenant_id,
            tool_name=tool_name,
            timestamp=datetime.utcnow(),
            latency_ms=latency_ms,
            tokens_used=tokens_used,
            success=success,
            error=error,
            cost=cost,
        )

        self.usage_records.append(record)

        # Update quota
        if tenant_id in self.tenant_quotas:
            quota = self.tenant_quotas[tenant_id]
            quota.call_count += 1
            quota.cost_accumulated += cost

        return True

    def _calculate_cost(
        self,
        tool_name: str,
        latency_ms: float,
        tokens_used: int,
    ) -> float:
        """Calculate cost for a tool call."""
        if tool_name not in self.pricing:
            return 0.0

        pricing = self.pricing[tool_name]
        cost = 0.0

        if pricing.model == BillingModel.PER_REQUEST:
            cost = pricing.per_call

        elif pricing.model == BillingModel.PER_LATENCY:
            cost = latency_ms * pricing.per_latency_ms

        elif pricing.model == BillingModel.HYBRID:
            cost = pricing.per_call + (latency_ms * pricing.per_latency_ms)

        # Add token cost
        if tokens_used > 0:
            cost += tokens_used * pricing.per_token

        # Apply minimum and maximum
        cost = max(cost, pricing.minimum_charge)
        if pricing.maximum_charge:
            cost = min(cost, pricing.maximum_charge)

        return cost

    def _check_quota(self, tenant_id: str, additional_cost: float) -> bool:
        """Check if tenant is within quota."""
        if tenant_id not in self.tenant_quotas:
            return True

        quota = self.tenant_quotas[tenant_id]

        # Check call quota
        if quota.max_calls_per_period and quota.call_count >= quota.max_calls_per_period:
            return False

        # Check cost quota
        if quota.max_cost_per_period and (quota.cost_accumulated + additional_cost) > quota.max_cost_per_period:
            return False

        return True

    def get_usage_report(
        self,
        tenant_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Generate usage report for tenant."""
        if start_date is None:
            start_date = datetime.utcnow() - timedelta(days=30)
        if end_date is None:
            end_date = datetime.utcnow()

        # Filter records
        records = [
            r for r in self.usage_records
            if r.tenant_id == tenant_id and start_date <= r.timestamp <= end_date
        ]

        # Calculate statistics
        total_calls = len(records)
        successful_calls = len([r for r in records if r.success])
        failed_calls = total_calls - successful_calls
        total_cost = sum(r.cost for r in records)
        total_latency = sum(r.latency_ms for r in records)
        avg_latency = total_latency / total_calls if total_calls > 0 else 0

        # Group by tool
        by_tool = {}
        for record in records:
            if record.tool_name not in by_tool:
                by_tool[record.tool_name] = {
                    "calls": 0,
                    "successful": 0,
                    "failed": 0,
                    "cost": 0.0,
                    "total_latency_ms": 0.0,
                }

            by_tool[record.tool_name]["calls"] += 1
            if record.success:
                by_tool[record.tool_name]["successful"] += 1
            else:
                by_tool[record.tool_name]["failed"] += 1
            by_tool[record.tool_name]["cost"] += record.cost
            by_tool[record.tool_name]["total_latency_ms"] += record.latency_ms

        return {
            "tenant_id": tenant_id,
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat(),
            "total_calls": total_calls,
            "successful_calls": successful_calls,
            "failed_calls": failed_calls,
            "success_rate": (successful_calls / total_calls * 100) if total_calls > 0 else 0,
            "total_cost": total_cost,
            "average_latency_ms": avg_latency,
            "by_tool": by_tool,
        }

    def get_billing_invoice(
        self,
        tenant_id: str,
        period: str,  # "2026-08"
    ) -> Dict[str, Any]:
        """Generate billing invoice for period."""
        # Parse period
        year, month = map(int, period.split("-"))
        start_date = datetime(year, month, 1)

        # Calculate end date
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        report = self.get_usage_report(tenant_id, start_date, end_date)

        return {
            "invoice_id": f"INV-{tenant_id}-{period}",
            "tenant_id": tenant_id,
            "period": period,
            "issued_at": datetime.utcnow().isoformat(),
            "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "summary": {
                "total_calls": report["total_calls"],
                "total_cost": report["total_cost"],
                "success_rate": report["success_rate"],
            },
            "line_items": [
                {
                    "tool": tool,
                    "calls": stats["calls"],
                    "successful": stats["successful"],
                    "failed": stats["failed"],
                    "cost": stats["cost"],
                }
                for tool, stats in report["by_tool"].items()
            ],
            "details": report,
        }

    def export_usage_records(self) -> str:
        """Export usage records as JSON for archival."""
        records = [
            {
                "tenant_id": r.tenant_id,
                "tool_name": r.tool_name,
                "timestamp": r.timestamp.isoformat(),
                "latency_ms": r.latency_ms,
                "tokens_used": r.tokens_used,
                "success": r.success,
                "error": r.error,
                "cost": r.cost,
            }
            for r in self.usage_records
        ]
        return json.dumps(records, indent=2)

    def get_tenant_total_cost(self, tenant_id: str, period_days: int = 30) -> float:
        """Get total cost for tenant in last N days."""
        cutoff_date = datetime.utcnow() - timedelta(days=period_days)
        return sum(
            r.cost for r in self.usage_records
            if r.tenant_id == tenant_id and r.timestamp >= cutoff_date
        )

    def get_top_tools_by_cost(self, tenant_id: str, limit: int = 10) -> List[Dict]:
        """Get top tools by cost for tenant."""
        by_tool = {}
        for record in self.usage_records:
            if record.tenant_id == tenant_id:
                if record.tool_name not in by_tool:
                    by_tool[record.tool_name] = 0.0
                by_tool[record.tool_name] += record.cost

        sorted_tools = sorted(by_tool.items(), key=lambda x: x[1], reverse=True)
        return [
            {"tool": tool, "cost": cost}
            for tool, cost in sorted_tools[:limit]
        ]

    def reset_tenant_quota(self, tenant_id: str) -> None:
        """Reset tenant's quota counters."""
        if tenant_id in self.tenant_quotas:
            quota = self.tenant_quotas[tenant_id]
            quota.call_count = 0
            quota.cost_accumulated = 0.0
            quota.period_start = datetime.utcnow()
