# srpo

Simple Remote Python Objects (SRPO) enables the sharing of objects between
processes. It works by forking the process and allowing proxies to interact with
the objects in a synchronous way.   

Quickstart
---------

```python
import srpo

obj = {'shared': 2}

proxy1 = srpo.transcend(obj, 'obj')
proxy2 = srpo.get_proxy('obj')  # this works the same from a separate process

assert proxy1['shared'] == proxy2['shared'] == 2

proxy2['another_attr'] = 3
assert proxy1['another_attr'] == 3

```
