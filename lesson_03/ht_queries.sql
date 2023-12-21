/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

SELECT
				c."name"		AS category_name
		  	  , COUNT(*)		AS number_films
FROM 			film_category fc 
LEFT JOIN		category c 
				USING(category_id)
GROUP BY 		"name"
ORDER BY 		number_films DESC;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/

SELECT
				a.first_name || ' ' || a.last_name  	AS actor
			  , COUNT(*)								AS films_rented
FROM			rental r 
LEFT JOIN		inventory i 
				USING(inventory_id)
LEFT JOIN		film_actor fa 
				USING(film_id)
LEFT JOIN		actor a 
				USING(actor_id)
GROUP BY		1
ORDER BY 		films_rented DESC
LIMIT 			10;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

SELECT
				c."name" 		AS category_name
			  , SUM(p.amount)	AS amount
FROM 			payment p 
LEFT JOIN		rental r 
				USING(rental_id)
LEFT JOIN		inventory i 
				USING(inventory_id)
LEFT JOIN		film_category fc 
				USING(film_id)
LEFT JOIN		category c 
				USING(category_id)
GROUP BY		c."name"
ORDER BY		amount DESC
LIMIT 1;

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

SELECT
				f.title
FROM			film f 
LEFT JOIN		inventory i 
				USING(film_id)
WHERE			i.film_id IS NULL;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

-- оскільки на 3-му місці декілька акторів з кількістю фільмів 5 - виведемо їх усіх

WITH children_actors_data AS (
-- знаходимо акторів які зʼявлялись в категорії фільмів “Children”
-- і рахуємо кількість фільмів

	SELECT
					a.first_name || ' ' || a.last_name 		AS actor_name
				  , COUNT(*)								AS number_films
	FROM			film_category fc 
	LEFT JOIN		category c 
					USING(category_id)
	LEFT JOIN		film f 
					USING(film_id)
	LEFT JOIN		film_actor fa 
					USING(film_id)
	LEFT JOIN		actor a 
					USING(actor_id)
	WHERE 			c."name" = 'Children'
	GROUP BY 		1
)

, adding_place_number AS (
-- додаємо зайняте місце в рейтингу

	SELECT
					*
				  , ROW_NUMBER() OVER(ORDER BY number_films DESC) 	AS place
	FROM 			children_actors_data
)

-- виводимо акторів і кількість фільмів де кількість фільмів 
-- більша або дорівнює кількості фільмів у 3-го місця

SELECT
					actor_name
				  , number_films
FROM				children_actors_data
WHERE				number_films >= (SELECT MAX(number_films) FROM adding_place_number WHERE place = 3)
ORDER BY 			number_films DESC;
