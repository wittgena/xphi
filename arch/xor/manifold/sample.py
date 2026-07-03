# arch.xor.manifold.sample
from pydantic import BaseModel

class Sample:
    def __init__(self, base=None, **kwargs):
        self._store = {}
        self._demos = []
        self._input_keys = None

        if base and isinstance(base, type(self)):
            self._store = base._store.copy()
        elif base and isinstance(base, dict):
            self._store = base.copy()

        self._store.update(kwargs)

    def __getattr__(self, key):
        if key.startswith("__") and key.endswith("__"):
            raise AttributeError
        if key in self._store:
            return self._store[key]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'")

    def __setattr__(self, key, value):
        if key.startswith("_") or key in dir(self.__class__):
            super().__setattr__(key, value)
        else:
            self._store[key] = value

    def __getitem__(self, key):
        return self._store[key]

    def __setitem__(self, key, value):
        self._store[key] = value

    def __delitem__(self, key):
        del self._store[key]

    def __contains__(self, key):
        return key in self._store

    def __len__(self):
        return len([k for k in self._store if not k.startswith("spi_")])

    def __repr__(self):
        d = {k: v for k, v in self._store.items() if not k.startswith("spi_")}
        return f"Example({d})" + f" (input_keys={self._input_keys})"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        return isinstance(other, Sample) and self._store == other._store

    def __hash__(self):
        return hash(tuple(self._store.items()))

    def keys(self, include_spi=False):
        return [k for k in self._store.keys() if not k.startswith("spi_") or include_spi]

    def values(self, include_spi=False):
        return [v for k, v in self._store.items() if not k.startswith("spi_") or include_spi]

    def items(self, include_spi=False):
        return [(k, v) for k, v in self._store.items() if not k.startswith("spi_") or include_spi]

    def get(self, key, default=None):
        return self._store.get(key, default)

    def with_inputs(self, *keys):
        copied = self.copy()
        copied._input_keys = set(keys)
        return copied

    def inputs(self):
        if self._input_keys is None:
            raise ValueError("Inputs have not been set for this example. Use `example.with_inputs()` to set them.")

        d = {key: self._store[key] for key in self._store if key in self._input_keys}
        new_instance = type(self)(base=d)
        new_instance._input_keys = self._input_keys
        return new_instance

    def labels(self):
        input_keys = self.inputs().keys()
        d = {key: self._store[key] for key in self._store if key not in input_keys}
        return type(self)(d)

    def __iter__(self):
        return iter(dict(self._store))

    def copy(self, **kwargs):
        return type(self)(base=self, **kwargs)

    def without(self, *keys):
        copied = self.copy()
        for key in keys:
            del copied[key]
        return copied

    def toDict(self):
        def convert_to_serializable(value):
            if hasattr(value, "toDict"):
                return value.toDict()
            elif isinstance(value, BaseModel):
                return value.model_dump()
            elif isinstance(value, list):
                return [convert_to_serializable(item) for item in value]
            elif isinstance(value, dict):
                return {k: convert_to_serializable(v) for k, v in value.items()}
            else:
                return value

        serializable_store = {}
        for k, v in self._store.items():
            serializable_store[k] = convert_to_serializable(v)

        return serializable_store

class Prediction(Sample):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        del self._demos
        del self._input_keys

        self._completions = None
        self._lm_usage = None

    def get_lm_usage(self):
        return self._lm_usage

    def set_lm_usage(self, value):
        self._lm_usage = value

    @classmethod
    def from_completions(cls, list_or_dict, signature=None):
        obj = cls()
        obj._completions = Completions(list_or_dict, signature=signature)
        obj._store = {k: v[0] for k, v in obj._completions.items()}

        return obj

    def __repr__(self):
        store_repr = ",\n    ".join(f"{k}={v!r}" for k, v in self._store.items())

        if self._completions is None or len(self._completions) == 1:
            return f"Prediction(\n    {store_repr}\n)"

        num_completions = len(self._completions)
        return f"Prediction(\n    {store_repr},\n    completions=Completions(...)\n) ({num_completions-1} completions omitted)"

    def __str__(self):
        return self.__repr__()

    def __float__(self):
        if "score" not in self._store:
            raise ValueError("Prediction object does not have a 'score' field to convert to float.")
        return float(self._store["score"])

    def __add__(self, other):
        if isinstance(other, (float, int)):
            return self.__float__() + other
        elif isinstance(other, Prediction):
            return self.__float__() + float(other)
        raise TypeError(f"Unsupported type for addition: {type(other)}")

    def __radd__(self, other):
        if isinstance(other, (float, int)):
            return other + self.__float__()
        elif isinstance(other, Prediction):
            return float(other) + self.__float__()
        raise TypeError(f"Unsupported type for addition: {type(other)}")

    def __truediv__(self, other):
        if isinstance(other, (float, int)):
            return self.__float__() / other
        elif isinstance(other, Prediction):
            return self.__float__() / float(other)
        raise TypeError(f"Unsupported type for division: {type(other)}")

    def __rtruediv__(self, other):
        if isinstance(other, (float, int)):
            return other / self.__float__()
        elif isinstance(other, Prediction):
            return float(other) / self.__float__()
        raise TypeError(f"Unsupported type for division: {type(other)}")

    def __lt__(self, other):
        if isinstance(other, (float, int)):
            return self.__float__() < other
        elif isinstance(other, Prediction):
            return self.__float__() < float(other)
        raise TypeError(f"Unsupported type for comparison: {type(other)}")

    def __le__(self, other):
        if isinstance(other, (float, int)):
            return self.__float__() <= other
        elif isinstance(other, Prediction):
            return self.__float__() <= float(other)
        raise TypeError(f"Unsupported type for comparison: {type(other)}")

    def __gt__(self, other):
        if isinstance(other, (float, int)):
            return self.__float__() > other
        elif isinstance(other, Prediction):
            return self.__float__() > float(other)
        raise TypeError(f"Unsupported type for comparison: {type(other)}")

    def __ge__(self, other):
        if isinstance(other, (float, int)):
            return self.__float__() >= other
        elif isinstance(other, Prediction):
            return self.__float__() >= float(other)
        raise TypeError(f"Unsupported type for comparison: {type(other)}")

    @property
    def completions(self):
        return self._completions

class Completions:
    def __init__(self, list_or_dict, signature=None):
        self.signature = signature

        if isinstance(list_or_dict, list):
            kwargs = {}
            for arg in list_or_dict:
                for k, v in arg.items():
                    kwargs.setdefault(k, []).append(v)
        else:
            kwargs = list_or_dict

        assert all(isinstance(v, list) for v in kwargs.values()), "All values must be lists"

        if kwargs:
            length = len(next(iter(kwargs.values())))
            assert all(len(v) == length for v in kwargs.values()), "All lists must have the same length"

        self._completions = kwargs

    def items(self):
        return self._completions.items()

    def __getitem__(self, key):
        if isinstance(key, int):
            if key < 0 or key >= len(self):
                raise IndexError("Index out of range")

            return Prediction(**{k: v[key] for k, v in self._completions.items()})

        return self._completions[key]

    def __getattr__(self, name):
        if name == "_completions":
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        if name in self._completions:
            return self._completions[name]

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __len__(self):
        return len(next(iter(self._completions.values())))

    def __contains__(self, key):
        return key in self._completions

    def __repr__(self):
        items_repr = ",\n    ".join(f"{k}={v!r}" for k, v in self._completions.items())
        return f"Completions(\n    {items_repr}\n)"

    def __str__(self):
        return self.__repr__()