import os


def main():
    output = os.popen('ps x | grep imaging').read()
    print(output)

if __name__ == "__main__":
    main()
