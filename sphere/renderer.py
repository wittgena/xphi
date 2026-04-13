# sphere.renderer

class MetalogRenderer:
    """@role: Φ′ projection (Metalog state -> Prometheus representation)"""
    METRICS = [
        {
            "name": "metalog_keys_active_total",
            "type": "gauge",
            "help": "Number of unique event keys currently being tracked",
            "value": lambda s: s["total_keys"],
        },
        {
            "name": "metalog_pressure_max_density",
            "type": "gauge",
            "help": "Maximum event density observed across all keys",
            "value": lambda s: s["max_density"],
        },
        {
            "name": "metalog_pressure_avg_density",
            "type": "gauge",
            "help": "Average event density across all keys",
            "value": lambda s: s["avg_density"],
        },
        {
            "name": "metalog_folding_active_keys",
            "type": "gauge",
            "help": "Number of keys currently in 'folding' (suppression) state",
            "value": lambda s: s["active_folds"],
        },
        {
            "name": "metalog_folding_events_total",
            "type": "counter",
            "help": "Total count of events that have been folded (suppressed)",
            "value": lambda s: s["total_folded_count"],
        },
        {
            "name": "metalog_control_threshold",
            "type": "gauge",
            "help": "Current pressure threshold for folding activation",
            "value": lambda s: s["threshold"],
        },
    ]

    def render(self, s):
        lines = []
        for m in self.METRICS:
            v = m["value"](s)
            lines.append(f"# HELP {m['name']} {m['help']}")
            # summary나 counter의 경우도 gauge로 일단 표현 (시각화 목적)
            lines.append(f"# TYPE {m['name']} {m['type']}")
            lines.append(f"{m['name']} {v}")
            lines.append("")
        return "\n".join(lines)

class SystemRenderer:
    """@role: Φ′ projection (internal state -> prometheus representation)"""
    METRICS = [
        {
            "name": "cpu_usage_percent",
            "type": "gauge",
            "help": "CPU usage in percent",
            "value": lambda s: s["cpu_percent"],
        },
        {
            "name": "cpu_count",
            "type": "gauge",
            "help": "Number of logical CPUs",
            "value": lambda s: s["cpu_count"],
        },
        {
            "name": "cpu_user_percent",
            "type": "gauge",
            "help": "CPU user time percentage",
            "value": lambda s: s["cpu_times"].user,
        },
        {
            "name": "cpu_system_percent",
            "type": "gauge",
            "help": "CPU system time percentage",
            "value": lambda s: s["cpu_times"].system,
        },
        {
            "name": "memory_total_bytes",
            "type": "gauge",
            "help": "Total physical memory",
            "value": lambda s: s["mem"].total,
        },
        {
            "name": "memory_used_bytes",
            "type": "gauge",
            "help": "Used physical memory",
            "value": lambda s: s["mem"].used,
        },
        {
            "name": "memory_available_bytes",
            "type": "gauge",
            "help": "Available physical memory",
            "value": lambda s: s["mem"].available,
        },
        {
            "name": "memory_usage_percent",
            "type": "gauge",
            "help": "Memory usage percent",
            "value": lambda s: s["mem"].percent,
        },
        {
            "name": "swap_usage_percent",
            "type": "gauge",
            "help": "Swap memory usage percent",
            "value": lambda s: s["swap"].percent,
        },
        {
            "name": "disk_usage_percent",
            "type": "gauge",
            "help": "Disk usage percent for root",
            "value": lambda s: s["disk"].percent,
        },
        {
            "name": "disk_read_bytes",
            "type": "counter",
            "help": "Total disk read in bytes",
            "value": lambda s: s["disk_io"].read_bytes,
        },
        {
            "name": "disk_write_bytes",
            "type": "counter",
            "help": "Total disk written in bytes",
            "value": lambda s: s["disk_io"].write_bytes,
        },
        {
            "name": "net_bytes_sent",
            "type": "counter",
            "help": "Total bytes sent over network",
            "value": lambda s: s["net"].bytes_sent,
        },
        {
            "name": "net_bytes_recv",
            "type": "counter",
            "help": "Total bytes received over network",
            "value": lambda s: s["net"].bytes_recv,
        },
        {
            "name": "process_count",
            "type": "gauge",
            "help": "Number of running processes",
            "value": lambda s: s["proc_count"],
        },
    ]

    def render(self, s):
        lines = []
        for m in self.METRICS:
            v = m["value"](s)
            lines.append(f"# HELP {m['name']} {m['help']}")
            lines.append(f"# TYPE {m['name']} {m['type']}")
            lines.append(f"{m['name']} {v}")
            lines.append("")
        return "\n".join(lines)

class Renderer:
    """@role: Φ′ projection (Receptor Field state -> Prometheus representation)"""
    
    def render(self, state: dict):
        lines = []
        # 상태(state)는 FieldProjector에서 저장한 payload 딕셔너리들의 모음입니다.
        for payload in state.values():
            name = payload.get("kind")
            val = payload.get("value")
            labels = payload.get("labels", {})
            
            if not name or val is None:
                continue

            # Labels 포맷팅: {pod="worker-a", node="node-1"}
            label_str = ",".join([f'{k}="{v}"' for k, v in labels.items()])
            
            # TYPE 및 HELP는 메타데이터가 있다면 추가 가능하며, 여기서는 생략하거나 기본값 적용
            lines.append(f"# TYPE {name} gauge")
            if label_str:
                lines.append(f"{name}{{{label_str}}} {val}")
            else:
                lines.append(f"{name} {val}")
            lines.append("")
            
        return "\n".join(lines)