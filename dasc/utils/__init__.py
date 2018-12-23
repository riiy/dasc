import inspect
import os


def get_handlers(handler_path):
    handers = {}
    import importlib.util
    handler_path = os.path.abspath(handler_path)
    for f in os.listdir(handler_path):
        if os.path.isfile(os.path.join(handler_path, f)) and not f.startswith('__'):
            spec = importlib.util.spec_from_file_location(handler_path + os.sep + f, os.path.join(handler_path, f))
            foo = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(foo)
            handers.update({name: func for name, func in inspect.getmembers(foo, inspect.isfunction)})

    return handers


if __name__ == '__main__':
    print(get_handlers('.'))
