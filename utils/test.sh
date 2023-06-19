#!/usr/bin/env bash

while IFS='=' read -r key value; do
  if [[ $key == *"["* ]]; then
    section=$(echo $key | tr -d '[]')
  elif [[ $key != "" ]]; then
    if [[ $section == "tool.poetry.group.airflow.dependencies" ]]; then
        package=$(echo $key | tr -d '[:space:]')
        version=$(echo $value | tr -d '[:space:]"')
      if [[ $package != "python" ]]; then
          echo $package==$version >> requirements.txt
      fi
    fi
  fi
done < pyproject.toml