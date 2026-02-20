# This file conatins the Precision , latency and efficiency ( what measurements we care about?)


"""
Defining the evaluation metrics for Sentinnel Banking

This file contains what is measured. 
Actual computation is measured later.

"""

class Metrics:
    """
    Central registry of evaluation metrics

    """

    triage_precision = "triage_precision"
    """ 
    This is the percentage of complaints routed to the correct department.
    Goal: >95%
    """

    reasoning_clarity = "reasoning_clarity"
    """
    This checks if the agent provided a clear explanation 
    of its decision.
    """

    human_efficiency = "human_efficiency"
    """
    This compares the manual routing to the estimated hours saved
    per 100 tickets
    """
    