## Installation Process (Linux)

1. **Install Poetry**  
   Run the following command in your terminal to install Poetry:
   ```bash
   sudo apt install poetry
   ```
   If Poetry is already installed on your system, you can skip this step.

2. **Install Dependencies and Set Up Virtual Environment**  
   In the root directory of your project, execute:
   ```bash
   poetry install
   ```
   This will install a virtual environment and all dependencies.

3. **Create `.env` File**  
   Run:
   ```bash
   echo -e "IS_JWT_USER=\nIS_JWT=\nFMP_API_KEY=\n3CANDLES_DATA_CONFIG_PATH=configs/dataconfig.yaml\n3CANDLES_CONFIG_PATH=configs/config.yaml" | tee .env >/dev/null
   ```
   This command creates the `.env` file and fills it with initial data configuration. 
   If you skip this step, it will be created automatically.

4. **Edit `.env` File (Optional)**  
   Fill in the `IS_JWT=` `IS_JWT_USER=` `FMP_API_KEY=` value in `.env`. Also, edit the config paths if needed.

5. **Edit Config Files (Optional)**  
   Edit`dataconfig.yaml` in the `configs/` folder if you need to adjust settings.

6. **Run the Program**  
   Execute:
   ```bash
   poetry run python3 main.py
   ```
