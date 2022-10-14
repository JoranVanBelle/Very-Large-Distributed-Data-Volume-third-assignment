# Very-Large-Distributed-Data-Volume-third-assignment

## Login

In order to login to the VM, you have to be connected to the NTNU VPN. How to do this can be found [here](https://i.ntnu.no/wiki/-/wiki/English/Install+vpn).

Once you are connected with the VPN, you can login to the virtual machine.

### Login VM

1. Open your terminal
2. By entering ```ssh <your_username>@tdt4225-14.idi.ntnu.no``` where <your_username> is your username from NTNU.
  2.1. When asked is you want to continue, type ```yes```.
  2.2. When your password is asked, enter your Feide-password.
  2.3. You should be logged in now.
3. To open mysql, type ```sudo mysql```.
4. The credentials of the mysql user are sent in the Whatsapp group.

## Python

To be able to run the files inside the folder "exercise2-files", you should download the data to your own pc and place it in the main foler of the project.


### Install packages

1. install virtualenv.
2. Create and activate a virtual env: ```python -m venv .\venv\Scripts\activate```.
3. Install the required packages: ```python -m pip install -r requirements.txt```.
4. You also have to create a ```.env``` file before you can run the ```example.py```. The content of this file is sent in the Whatssapp group.

## MongoDB

### Server problems

Every time we want to work at the assignment, ``sudo mongod --bind_ip_all`` has to be entered in the VM.
To make it easier to work for us, I created a tmux and ran the command in there.

### Connect

0. If you had to run the command above, open a new terminal and ssh into the vm.
1. run ``sudo mongosh``
