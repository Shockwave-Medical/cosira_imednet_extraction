-- SFCS column name cleaning function (callable in SQL)
CREATE OR REPLACE FUNCTION clean_column_name(name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'clean_name'
AS
$$
import re

def clean_name(name):
    """SFCS column cleaning function"""
    if not name:
        return ''
        
    replace_newlines_tabs_with_space_re = r'[\r\n\t]+'
    remove_leading_digits_dots_spaces_re = r'^[0-9]+[\s\.]*'
    remove_special_chars_except_alnum_space_re = r'[^a-zA-Z0-9\s]'
    replace_spaces_with_underscore_re = r'\s+'

    replacements = [
        (replace_newlines_tabs_with_space_re, ' '),
        (remove_leading_digits_dots_spaces_re, ''),
        (remove_special_chars_except_alnum_space_re, ''),
        (replace_spaces_with_underscore_re, '_'),
    ]

    for pattern, replacement in replacements:
        name = re.sub(pattern, replacement, name)
    return name.upper()
$$;