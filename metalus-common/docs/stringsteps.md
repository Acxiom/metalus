[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# StringSteps
This object exposes some basic string functions.

##To String
Calls the toString method of any object passed. Can unwrap options.

* **value** - Object to call toString on.
* **unwrapOption** - Optional flag to enable Option unwrapping. Defaulted to false.

## List To String
Given a list, will call the mkString function.

* **list** - List to call mkString on.
* **separator** - Optional separator to be used.
* **unwrapOption** - Optional flag to enable Option unwrapping. Defaulted to false.


##Uppercase
Calls "uppercase" on the given string.

* **value** - The string to uppercase.

##Lowercase
Calls "lowercase" on the given string.

* **value** - The string to lowercase.

##String Split
Splits the given string into a list based on the given regex.

* **string** - The string to split.
* **regex** - Regex used to split te string. Can also be a simple separator.
* **limit** - Optional limit on the number of times the pattern is applied.

##Substring
Returns a substring based on the given string.

* **string** - The string to use.
* **begin** - The start index of the substring.
* **end** - Optional end index. If not provided, will stretch to the end of the string.

##String Equals
Returns whether the two given strings are equals. Can perform a case insensitive operation.

* **string** - First string to compare.
* **anotherString** - Second string to compare.
* **caseInsensitive** - Optional flag to indicate whether a case insensitive comparison should be performed.
 Defaults to false.
 
##String Matches
Returns whether the given string is matched by the provided regex.

* **string** - String to match.
* **regex** - Pattern to use.
