# Go exercise

This is a coding exercise to show your skills in handling CSV files using the Go language.

## Program structure and rules

The entrypoint of your program should be in `main.go`: it receives a directory containing CSVs, reads each file, combines and merge them, outputting the final CSV to stdout.

You can run it using the supplied test data, like:

```
go run main.go -dir data/simple
```

A few things to keep in mind about the data you'll work with:

- The first column on every CSV is the "Id", which you can use to match different fragments of the same row
- There's no guarantee the rows will be sorted
- There's no guarantee the files are sorted in any meaningful way, either

And rules about the data you'll produce:

- Please sort the rows by their ID, for convenience
- If there are gaps in the data, fill in with blanks

## Example

Given 4 CSV files:

```
Id,FirstName,LastName
1,Amy,Adams
2,John,Malkovich
```

```
Id,Phone,Email
1,310-111-1111,contact@amyadams.com
2,213-222-2222,john@malkovich.com
```

```
Id,FirstName,LastName
3,Larry,David
4,Michelle,Wolf
```

```
Id,Phone,Email
4,213-444-444,mwolf@comcast.net
```

The consolidated CSV could be:

```
Id,FirstName,LastName,Phone,Email
1,Amy,Adams,310-111-1111,contact@amyadams.com
2,John,Malkovich,213-222-2222,john@malkovich.com
3,Larry,David,,
4,Michelle,Wolf,213-444-444,mwolf@comcast.net
```

Note the columns could be in a different order: `Id,Phone,Email,FirstName,Lastname`.

