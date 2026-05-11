from django import template

register = template.Library()


@register.filter(name="lookup")
def lookup(d, key):
    if d is None:
        return ""
    try:
        return d.get(key, "")
    except AttributeError:
        return ""
