import re
import random

#fix: how to get the ^ inside a bracket, ex: [^abc]

#parse the given regex to different, non-overlapping parts
def parse_regex(regex):
    pattern = re.compile(r'(\[.*?\]|\\.|{[^{}]+}|.)') #ex:'[0-9]{3}-[A-Za-z]{2}{2,3}' will be parsed into ['[0-9]', '{3}', '-', '[A-Za-z]', '{2}', '{2,3}']

    return pattern.findall(regex)

# Function to modify a token with a range, ensure first digit is smaller than second digit
def modify_token_with_range(token):
    match = re.match(r'{(\d+),(\d+)}', token)
    if match:
        first_digit, second_digit = map(int, match.groups())
        new_first_digit = random.randint(0, 9)
        new_second_digit = random.randint(new_first_digit, 9)
        modified_token = token.replace(f'{first_digit},{second_digit}', f'{new_first_digit},{new_second_digit}')
        return modified_token
    return token

# Function to modify the regex tokens
def modify_regex(tokens, invalid_count):
    modified_tokens = tokens.copy()

    for _ in range(invalid_count):
        index_to_modify = random.randint(0, len(modified_tokens) - 1)
        token_to_modify = modified_tokens[index_to_modify]

        while token_to_modify in ['(', ')', '$', '^']:
            index_to_modify = random.randint(0, len(modified_tokens) - 1)
            token_to_modify = modified_tokens[index_to_modify]

        # If the token matches the format {num1, num2}, modify it using the range modification function
        if re.match(r'{\d+,\d+}', token_to_modify):
            modified_tokens[index_to_modify] = modify_token_with_range(token_to_modify)
        else:
            # Modify the token with constraints
            modified_tokens[index_to_modify] = modify_token(token_to_modify)

    # Reconstruct the modified regex
    modified_regex = ''.join(modified_tokens)

    return modified_regex


def modify_token(token):
    # Replace metacharacters with alternatives
    replacements = {
        '?': ['.', '+'],
        '\s': ['\d','\D','\w','\W'],
        '\d': ['\s','\D','\w','\W'],
        '[\s]': ['[\d]','[\D]','[\w]','[\W]'],
        '[\d]': ['[\s]','[\D]','[\w]','[\W]'],
        '[A-Z]': ['[a-z]', '[0-9]'],
        '[a-z]': ['[A-Z]', '[0-9]'],
        '[0-9]': ['[A-Za-z]'],
        '[A-Za-z]': ['[0-9]'],
        '[0123456789]':['[A-Za-z]'],
        '-':['\\','/','\.', ' '],
        '\.':['\\','-','/', ' '],
        '\\':['\.','-','/', ' '],
        '[/]':['\.','-','\\', ' '],
        '[-]': ['[\\]', '[/]', '[\.]','[ ]','[.]'],
        '[\.]': ['[\\]', '[-]', '[/]', '[ ]','[.]'],
        '[\\]': ['[\.]', '[-]', '[/]','[ ]','[.]'],
        '[/]': ['[\.]', '[-]', '[\\]','[ ]','[.]'],
        '[ ]': ['[\.]', '[.]','[-]', '[\\]','[/]'],
        '[.]': ['[\.]', '[-]', '[\\]','[ ]','[/]'],
        '{0}': ['{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}'],
        '{1}': ['{0}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}'],
        '{2}': ['{0}', '{1}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}'],
        '{3}': ['{0}', '{1}', '{2}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}'],
        '{4}': ['{0}', '{1}', '{2}', '{3}', '{5}', '{6}', '{7}', '{8}', '{9}'],
        '{5}': ['{0}', '{1}', '{2}', '{3}', '{4}', '{6}', '{7}', '{8}', '{9}'],
        '{6}': ['{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{7}', '{8}', '{9}'],
        '{7}': ['{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{8}', '{9}'],
        '{8}': ['{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{9}'],
        '{9}': ['{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}']
    }
    

    print(f'Token picked: {token}')
    
    # Apply replacements
    if token in replacements:
        replacement_options = replacements[token]
        replacement = random.choice(replacement_options)
        print(f'replacement choice picked: {replacement}')

        token = replacement
    return token

# Example regular expression
original_regex = "^([0123456789]{2}[.][0123456789]{3}[.][\d]{3})$"

# '(\d{2}|1[012]\d)(\.\d{0,20}){0,1})'

# Parse the original regex into tokens
parsed_tokens = parse_regex(original_regex)

# number of modifications (invalid regex changes) wanted
invalid_count = 2

# Modify the regex with constraints
modified_regex = modify_regex(parsed_tokens, invalid_count)

print(f'Original Regex: {original_regex}')
print(f'Modified Regex: {modified_regex}')
