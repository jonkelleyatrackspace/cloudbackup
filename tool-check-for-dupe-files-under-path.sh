# Finds duplicate filenames under a tree.

# USAGE: myname.sh /path/to/find/dupes/in/.


find $1 | sed 's/.*\///' | sort | uniq -c | sort -n > sorted.txt
echo "-> Log data accumulated."
echo "->   Sort logs and display them?"
read x
grep -v '      1' sorted.txt  | less


