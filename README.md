# srpo

Simple Remote Python Objects (SRPO) enables the sharing of objects between
processes. It works by forking the process and allowing proxies to interact with
the objects in a synchronous way. It was made for solving problems related to
concurrent writes to HDF5 files as implemented by 
[obsplus](www.github.com/niosh-mining/obsplus).

## Quickstart

```python
import srpo

# define an object we want to share between processes (a dict) and "transcend" it,
# meaning it is transfer to its own process. A proxy is returned. 
proxy1 = srpo.transcend({'shared': 2}, 'obj')

# From any process on the same machine we can access the object with its id like so
proxy2 = srpo.get_proxy('obj')

# Then manipulate the object as if it was (almost) local.
assert proxy1['shared'] == proxy2['shared'] == 2

proxy2['another_attr'] = 3
assert proxy1['another_attr'] == 3
```
