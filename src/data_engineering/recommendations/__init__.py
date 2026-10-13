"""Multi-agent cooling recommendation workflow (LangGraph + Fireworks LLMs + deterministic tools).

Recommendation-only: no code path in this package sends commands to fans, chillers, setpoints or
any other equipment. LLMs plan, interpret and explain; every number comes from deterministic
Python (the cooling state estimator, the XGBoost forecaster and the optimizer).
"""
