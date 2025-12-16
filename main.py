import sys
from getdata_insightsentry import make_csv
from getdata_FMP import make_csv_FMP

def user_make_decision():
    decision = input("\nWhat do you want to do?\n1 : Getdata from Insightsentry.\n2 : Getdata from FMP.\n3 : Exit\n")
    decision = decision.strip()
    if decision not in ['1', '2', '3']:
        print("Please make correct choise!")
    elif decision == '1':
        make_csv()
    elif decision == '2':
        make_csv_FMP()
    elif decision == '3':
        sys.exit("\nBye Bye!")
    user_make_decision()

if __name__ == "__main__":
    user_make_decision()
