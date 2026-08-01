# watcher.plane.metric.base
import numpy as np

def _is_invalid(values: np.ndarray, min_length: int = 2) -> bool:
    """
    Helper function to safely check if the input array is valid for calculations.
    Returns True if the array is None or its length is less than min_length.
    """
    if values is None or len(values) < min_length:
        return True
    return False


def trend_slope(values: np.ndarray) -> float:
    """1st-order Velocity (Linear Trend)"""
    if _is_invalid(values, 2):
        return 0.0
    return float(np.polyfit(range(len(values)), values, 1)[0])


def acceleration(values: np.ndarray) -> float:
    """2nd-order Acceleration (Momentum)"""
    ## Needs at least 3 points for a meaningful 2nd-order polynomial fit
    if _is_invalid(values, 3):
        return 0.0
    return float(np.polyfit(range(len(values)), values, 2)[0])


def range_amplitude(values: np.ndarray) -> float:
    """Maximum Amplitude (Absolute spatial footprint)"""
    if _is_invalid(values, 1):
        return 0.0
    return float(np.ptp(values))


"""Topological & Energy"""
def path_length(values: np.ndarray) -> float:
    """Actual distance of the trajectory (Friction)"""
    if _is_invalid(values, 2):
        return 0.0
    return float(np.sum(np.abs(np.diff(values))))

def mean_crossings(values: np.ndarray) -> float:
    """Number of times crossing the mean (Vibration Frequency)"""
    if _is_invalid(values, 2):
        return 0.0
    mean_val = np.mean(values)
    centered = values - mean_val
    return float((centered[:-1] * centered[1:] < 0).sum())

def signal_energy(values: np.ndarray) -> float:
    """Total kinetic energy of the signal (Sum of squared variations)"""
    if _is_invalid(values, 2):
        return 0.0
    return float(np.sum(np.diff(values)**2))

"""Distribution & Tail"""
def volatility(values: np.ndarray) -> float:
    """Dispersion of the trajectory (Standard Deviation of returns)"""
    if _is_invalid(values, 2):
        return 0.0
    denominator = values[:-1]
    
    ## Safe division: Prevent division by zero and invalid values (NaN/Inf)
    r = np.divide(
        np.diff(values), 
        denominator, 
        out=np.zeros_like(denominator, dtype=float), 
        where=(denominator != 0)
    )
    return float(np.std(r))


def drawdown(values: np.ndarray) -> float:
    """Maximum drawdown from the peak (Gravity/Collapse tendency)"""
    if _is_invalid(values, 1):
        return 0.0
        
    max_val = values.max()
    
    ## Safe division: Prevent division by zero if the maximum value is 0
    if max_val == 0:
        return 0.0
        
    return float((values.min() - max_val) / max_val)


def skewness(values: np.ndarray) -> float:
    """Asymmetry of the distribution (Directional force bias)"""
    if _is_invalid(values, 2):
        return 0.0
        
    diffs = np.diff(values)
    mean_d = np.mean(diffs)
    std_d = np.std(diffs)
    
    ## Safe division: Prevent division by zero if standard deviation is 0 (flat line)
    if std_d == 0:
        return 0.0
    return float(np.mean(((diffs - mean_d) / std_d)**3))