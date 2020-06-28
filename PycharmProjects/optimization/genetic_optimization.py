
import numpy as np
import pandas as pd


def get_new_population(**kwargs):

    num_individuals = kwargs['num_individuals']
    GeneticOptimizer = kwargs['GeneticOptimizer']

    return [GeneticOptimizer.get_random_parameters() for i in range(num_individuals)]


def calc_fitness(**kwargs):

    parameter_vector = kwargs['parameter_vector']
    GeneticOptimizer = kwargs['GeneticOptimizer']
    return GeneticOptimizer.get_fitness(parameter_vector)


def crossover(**kwargs):

    parent_list = kwargs['parent_list']
    offspring_size = kwargs['offspring_size']

    num_genes = len(parent_list[0])
    gene_list = list(range(num_genes))
    num_parents = len(parent_list)
    first_parent_num_genes = round(num_genes/2)

    offspring_list = []

    for k in range(offspring_size):

        parent1_genes = parent_list[k % num_parents]
        parent2_genes = parent_list[(k+1) % num_parents]
        child_genes = []

        first_parent_gene_list = np.random.choice(gene_list, first_parent_num_genes)

        for i in range(num_genes):

            if i in first_parent_gene_list:
                child_genes.append(parent1_genes[i])
            else:
                child_genes.append(parent2_genes[i])

        offspring_list.append(child_genes)

    return offspring_list


def mutation(**kwargs):

    offspring = kwargs['offspring']
    GeneticOptimizer = kwargs['GeneticOptimizer']

    gene_list = list(range(len(offspring)))
    mutative_gene = np.random.choice(gene_list, 1)
    random_genes = GeneticOptimizer.get_random_parameters()
    offspring[mutative_gene[0]] = random_genes[mutative_gene[0]]

    return offspring


def run_algo(**kwargs):

    GeneticOptimizer = kwargs['GeneticOptimizer']
    num_individuals = kwargs['num_individuals']

    num_mating_pool = 4
    num_generations = 5

    new_population = get_new_population(num_individuals=num_individuals, GeneticOptimizer=GeneticOptimizer)
    fitness_list = []

    for i in range(num_individuals):
        fitness_list.append(calc_fitness(GeneticOptimizer=GeneticOptimizer, parameter_vector=new_population[i]))

    old_generation = new_population

    for i in range(num_generations):
        output = create_new_generation(population=old_generation, fitness_list=fitness_list, GeneticOptimizer=GeneticOptimizer,num_mating_pool=num_mating_pool)
        old_generation = output['generation']
        fitness_list = output['fitness_list']

    fitness_frame = pd.DataFrame()
    fitness_frame['fitness'] = fitness_list
    fitness_frame['id'] = fitness_frame.index

    fitness_frame.sort_values(by=['fitness'], inplace=True, ascending=False)
    return old_generation[fitness_frame['id'].iloc[0]]


def create_new_generation(**kwargs):

    population = kwargs['population']
    fitness_list = kwargs['fitness_list']
    GeneticOptimizer = kwargs['GeneticOptimizer']
    num_mating_pool = kwargs['num_mating_pool']
    offspring_size = len(population)-num_mating_pool

    fitness_frame = pd.DataFrame()
    fitness_frame['fitness'] = fitness_list
    fitness_frame['id'] = fitness_frame.index

    fitness_frame.sort_values(by=['fitness'], inplace=True, ascending=False)

    mating_pool = []
    new_fitness_list = []

    for i in range(num_mating_pool):
        mating_pool.append(population[fitness_frame['id'].iloc[i]])
        new_fitness_list.append(fitness_list[fitness_frame['id'].iloc[i]])
    offspring_list = crossover(parent_list=mating_pool, offspring_size=offspring_size)
    offspring_list = [mutation(offspring=x, GeneticOptimizer=GeneticOptimizer) for x in offspring_list]

    for i in range(offspring_size):
        new_fitness_list.append(calc_fitness(GeneticOptimizer=GeneticOptimizer, parameter_vector=offspring_list[i]))

    mating_pool.extend(offspring_list)

    return {'generation': mating_pool, 'fitness_list': new_fitness_list}






