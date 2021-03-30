# Developer guide & notes

This document composes some guidelines and notes on development of Kafka-ADB, focusing on conventions used in the project.


## Code style
PostgreSQL code style is used.

`pgindent` is used to maintain codestyle.


## Single-statement `if`
The preferred style for `if`-statements that contain only one statement inside is "with brackets". However, both styles (with and without brackets) are currently in use.

The **preferred** style ("with brackets"):
```c
if (...)
{
      ...
}
```

The other style ("without brackets") which is permitted, but not encouraged:
```c
if (...)
      ...
```


## `volatile` in `PG_TRY()`
To start with, check out [this article](https://wiki.adsw.io/doku.php?id=projects:adb:%D0%BE%D1%88%D0%B8%D0%B1%D0%BA%D0%B0_%D0%BF%D1%80%D0%B8_%D0%B8%D1%81%D0%BF%D0%BE%D0%BB%D1%8C%D0%B7%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B8_pg_try) (in Russian).

All cases with `PG_TRY` **must** be resolved by either elimination of non-volatile variables (declaring them inside `PG_TRY()`) or by allocation of variables on heap.

However, there are cases when some variables do not change in `PG_TRY()`, or their state does not matter in `PG_CATCH()` (if they are pointers, the allocated memory is saved in some memory context anyway).

In such cases, `volatile` **may** be used. Currently, this modifier is used only with pointers. 

Think twice before applying this modifier! It requires extensive knowledge of `PG_TRY()`.

Example:
```c
List *volatile result = NIL;
...
```
