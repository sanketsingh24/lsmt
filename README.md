# lsm-tree


Overall TODO:

- do not export fields in structs which are not required, everything is fucking exported rn
- there are alot of pass by value instead of pass by refs, within structs, func args etc
- use things like `Bound[T]`, reduces memory usage :)
- many places uses values, pass by refs wherever possible
- someplaces, used []byte instead of value.Userkey and etc. fix it
- // iterate via channels@TODO:
- use path/filepath
