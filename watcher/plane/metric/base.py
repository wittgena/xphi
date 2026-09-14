import math
import statistics
from typing import Sequence

def _is_invalid(values: Sequence[float], min_length: int = 2) -> bool:
    if not values or len(values) < min_length:
        return True
    return False

def trend_slope(values: Sequence[float]) -> float:
    """1st-order Velocity (Linear Trend) - 단순 선형 회귀의 기울기"""
    if _is_invalid(values, 2):
        return 0.0
    n = len(values)
    sum_x = sum(range(n))
    sum_y = sum(values)
    sum_x2 = sum(x * x for x in range(n))
    sum_xy = sum(x * y for x, y in enumerate(values))
    
    denominator = (n * sum_x2 - sum_x ** 2)
    if denominator == 0:
        return 0.0
    return (n * sum_xy - sum_x * sum_y) / denominator

def acceleration(values: Sequence[float]) -> float:
    """2nd-order Acceleration (Momentum) - 2차 다항회귀의 최고차항(x^2) 계수"""
    if _is_invalid(values, 3):
        return 0.0
    
    n = len(values)
    # x = 0, 1, 2, ... n-1
    # 3x3 정규 방정식 행렬 (X^T * X * A = X^T * Y) 구성
    sx = sum(range(n))
    sx2 = sum(x**2 for x in range(n))
    sx3 = sum(x**3 for x in range(n))
    sx4 = sum(x**4 for x in range(n))
    sy = sum(values)
    sxy = sum(x * y for x, y in enumerate(values))
    sx2y = sum((x**2) * y for x, y in enumerate(values))
    
    # 3차 연립방정식 (Cramer's Rule 적용)
    # a*sx4 + b*sx3 + c*sx2 = sx2y
    # a*sx3 + b*sx2 + c*sx = sxy
    # a*sx2 + b*sx  + c*n  = sy
    D = (sx4 * (sx2 * n - sx * sx) - 
         sx3 * (sx3 * n - sx * sx2) + 
         sx2 * (sx3 * sx - sx2 * sx2))
         
    if D == 0:
        return 0.0
        
    Da = (sx2y * (sx2 * n - sx * sx) - 
          sxy * (sx3 * n - sx * sx2) + 
          sy * (sx3 * sx - sx2 * sx2))
          
    return Da / D

def range_amplitude(values: Sequence[float]) -> float:
    if _is_invalid(values, 1):
        return 0.0
    return max(values) - min(values)

def path_length(values: Sequence[float]) -> float:
    if _is_invalid(values, 2):
        return 0.0
    return sum(abs(values[i] - values[i-1]) for i in range(1, len(values)))

def mean_crossings(values: Sequence[float]) -> float:
    if _is_invalid(values, 2):
        return 0.0
    mean_val = sum(values) / len(values)
    centered = [v - mean_val for v in values]
    crossings = sum(1 for i in range(len(centered)-1) if centered[i] * centered[i+1] < 0)
    return float(crossings)

def signal_energy(values: Sequence[float]) -> float:
    if _is_invalid(values, 2):
        return 0.0
    return sum((values[i] - values[i-1])**2 for i in range(1, len(values)))

def volatility(values: Sequence[float]) -> float:
    if _is_invalid(values, 2):
        return 0.0
    
    returns = []
    for i in range(1, len(values)):
        denominator = values[i-1]
        if denominator != 0:
            returns.append((values[i] - denominator) / denominator)
        else:
            returns.append(0.0)
            
    if len(returns) < 2:
        return 0.0
    return statistics.pstdev(returns) # 모집단 표준편차 (np.std와 동일)

def drawdown(values: Sequence[float]) -> float:
    if _is_invalid(values, 1):
        return 0.0
    max_val = max(values)
    if max_val == 0:
        return 0.0
    return (min(values) - max_val) / max_val

def skewness(values: Sequence[float]) -> float:
    if _is_invalid(values, 2):
        return 0.0
        
    diffs = [values[i] - values[i-1] for i in range(1, len(values))]
    n = len(diffs)
    if n == 0:
        return 0.0
        
    mean_d = sum(diffs) / n
    # np.std와 동일한 모집단 표준편차
    variance = sum((d - mean_d)**2 for d in diffs) / n
    std_d = math.sqrt(variance)
    
    if std_d == 0:
        return 0.0
        
    skew = sum(((d - mean_d) / std_d)**3 for d in diffs) / n
    return float(skew)