import abc


class FeaturesAbstractFactory(metaclass=abc.ABCMeta):
    def __init__(self, **kwargs):
        self._obj = self._produce_feature_class(kwargs)

    @property
    def obj(self):
        return self._obj

    @abc.abstractmethod
    def _produce_feature_class(self, kwargs):
        pass

    def execute_feature_engineering(self):
        self._obj.feature_engineering()


class BasicInfoAbstractFactory(metaclass=abc.ABCMeta):
    def __init__(self, **kwargs):
        self._obj = self._produce_basic_info_class(kwargs)

    @property
    def obj(self):
        return self._obj

    @abc.abstractmethod
    def _produce_basic_info_class(self, kwargs):
        pass

    def parsing_and_push(self):
        self._obj.start_to_parse_msg()

