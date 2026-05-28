# CareerPilot Web Role Routing

CareerPilot Web uses an explicit role routing table in `careerpilot_api/role_router.py`.

Every feature task must register a `task_type` and map it to one professional `role_id` before it can produce product judgment. The route must also declare allowed read data, forbidden read data, allowed outputs, and states that the role must not modify.

When adding a new feature:

1. Add the new `task_type`.
2. Assign exactly one `role_id`.
3. Declare data read boundaries and output boundaries.
4. Add forbidden state changes, including no direct resume, preference, or application-status mutation.
5. Add or update tests before wiring the task into any UI or API workflow.

If no route matches, the system must use `general_product_advisor`. This fallback can only output ordinary explanations and routing guidance. It must not give final application decisions, resume rewrite conclusions, or job rankings.
