# Python's Libraries
import json
import logging


class DynamoAttrBase(object):

    def __str__(self):
        return self.get_Str(self.__value)

    def __repr__(self):
        return self.get_Str(self.__value)

    def __init__(self, _value=None):
        self.__value = self.set_Value(_value)

    @property
    def value(self):
        return self.__value

    @value.setter
    def value(self, _value):
        self.__value = self.set_Value(_value)


class DynamoStringAttr(DynamoAttrBase):

    def set_Value(self, _value):
        return str(_value) if _value else None

    def get_Str(self, _value):
        return _value if _value else ""


class DynamoNumberAttr(DynamoAttrBase):

    def set_Value(self, _value):
        return int(_value) if _value else None

    def get_Str(self, _value):
        if _value:
            return str(_value)
        else:
            return ""


class DynamoModelCollection(list):

    def fill(self, _list, _with_type=False):
        for item in _list:
            model = self.__model__()

            if "M" in item:
                model.fill(item['M'], _with_type=_with_type)
            else:
                model.fill(item, _with_type=_with_type)

            self.append(model)

    def get_Dict(self, _nulls=True):
        data = []
        for item in self:
            data.append(item.get_Dict(_nulls))

        return data


class DynamoModel(object):

    def __set_Attr(self, _attr_obj, _value, _with_type=False):
        if isinstance(_attr_obj, DynamoModelCollection):
            _attr_obj.fill(_value, _with_type=_with_type)

        else:
            _attr_obj.value = _value

    def fill(self, _data_dic, _with_type=False):
        attributes = self.__dict__.keys()

        for key, value in _data_dic.items():
            if key in attributes:
                attr_obj = getattr(self, key)

                if _with_type:
                    subkeys = value.keys()
                    subkeyvalue = None
                    for subk in subkeys:
                        subkeyvalue = value[subk]

                    self.__set_Attr(
                        attr_obj,
                        subkeyvalue,
                        _with_type=_with_type
                    )

                else:
                    self.__set_Attr(attr_obj, value)

    def get_Dict(self, _nulls=True):
        dict = {}
        for key, value in self.__dict__.items():
            attr_obj = getattr(self, key)

            if isinstance(attr_obj, DynamoModelCollection):
                if _nulls is False:
                    if value is None or value == []:
                        continue

                dict[key] = attr_obj.get_Dict()

            else:
                if _nulls is False:
                    if attr_obj.value is None or attr_obj.value == "":
                        continue

                dict[key] = attr_obj.value

        return dict


class DynamoModelSerializer(object):

    def __init__(self, _data=None, _many=False, _translate=False, _logger=None):
        self.data = _data
        self.many = _many
        self.translate = _translate
        self.logger = _logger or logging.getLogger(__name__)

    def __get_AttrToEval(self, _attr, _translate=None):
        if self.translate or _translate:
            if 'translate_list' not in dir(self):
                return _attr

            if self.translate_list is None:
                return _attr

            if _attr not in self.translate_list:
                return _attr

            return self.translate_list[_attr]

        else:
            return _attr

    def __get_LowerCamelCaseFormat(self, _value):
        new_value = ''.join(x.capitalize() or '_' for x in _value.split('_'))
        return new_value[0].lower() + new_value[1:]

    def __get_Label(self, _attr_name):
        if hasattr(self, "labels") is False:
            return None

        if _attr_name in self.labels.keys():
            return self.labels[_attr_name]
        else:
            return None

    def __get_ItemDict(self, _item, _translate=None):
        dict = {}
        class_attrs = _item.__dict__.keys()

        if 'list_attrs' not in dir(self):
            raise NameError(
                f"list_attrs is missing in class {self.__class__}"
            )

        for serial_attr in self.list_attrs:
            serial_attr_eval = self.__get_AttrToEval(
                serial_attr,
                _translate=_translate
            )

            if serial_attr_eval in class_attrs:
                attr_obj = getattr(_item, serial_attr_eval)
                value = ""

                if isinstance(attr_obj, DynamoModelCollection):
                    serializer_attr = getattr(self, serial_attr_eval)

                    if serializer_attr:
                        serializer_attr.data = attr_obj
                        serializer_attr.many = True
                        value = serializer_attr.get_Dict(self.translate)

                else:
                    value = attr_obj.value

                attr_label = self.__get_Label(serial_attr)

                if attr_label:
                    dict[attr_label] = value

                else:
                    dict[self.__get_LowerCamelCaseFormat(serial_attr)] = value

            else:
                raise NameError(f"{serial_attr_eval} is not in class {_item.__class__}")

        return dict

    def get_Dict(self, _translate=None):
        if self.many:
            list_data = []
            for dta in self.data:
                list_data.append(
                    self.__get_ItemDict(
                        dta,
                        _translate=_translate
                    )
                )

            if hasattr(self, "order_by"):
                if self.order_by:
                    if "-" in self.order_by:
                        list_data.sort(
                            key=lambda x: x.get(self.order_by.replace('-', '')),
                            reverse=True
                        )

                    else:
                        list_data.sort(key=lambda x: x.get(self.order_by))

            return list_data
        else:
            return self.__get_ItemDict(self.data)

    def get_Json(self):
        data = self.get_Dict()
        return json.dumps(data, ensure_ascii=False)
