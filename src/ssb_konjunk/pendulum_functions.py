"""
Helper functions for pendulum
"""


import pendulum

def month_bump(period: pendulum.DateTime, bump: int):
    """
    Function to help run code for more than one month at a time without changing the cls period.
    This code changes the format with "bump" amount of months.
    
    if bump = 1 and period = 202506
    then the output will be 202507

    works with possitive and negative numbers.

    Args:
        period: the pendulum datetime object
        bump: the amount of months that the period should be changed with
       
    Returns:
        int: the new period .
    """
    return period.add(months=bump).format("YYYYMM")