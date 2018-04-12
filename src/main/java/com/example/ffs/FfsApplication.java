package com.example.ffs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;

interface MovieRepository extends ReactiveMongoRepository<Movie, String> {

}

@SpringBootApplication
public class FfsApplication {

    public static void main(String[] args) {
        SpringApplication.run(FfsApplication.class, args);
    }


    @Bean
    CommandLineRunner movies(MovieRepository repository) {
        return args -> {

            repository.deleteAll().subscribe(null, null, () -> {
                Stream.of("Silence of the lambdas", "AEO Flux", "back to the future", "Fluxinator",
                        "Lord of fluxxes")
                        .forEach(movieTitle -> repository.save(
                                new Movie(UUID.randomUUID().toString(), movieTitle))
                                .subscribe(movie -> System.out.println(movie.toString())));
            });
        };
    }
}

@RestController
class FluxFlixRestController {
    private final FluxFlixService fluxFlixService;

    FluxFlixRestController(FluxFlixService fluxFlixService) {
        this.fluxFlixService = fluxFlixService;
    }
    @GetMapping("/movies")
    public Flux<Movie> all() {
        return fluxFlixService.all();
    }
    @GetMapping("/movies/{movieId}")
    public Mono<Movie> byId(@PathVariable String movieId) {
        return fluxFlixService.byId(movieId);
    }
    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE, value = "/movies/{movieId}/events")
    public Flux<MovieEvent> crosstheStreams(@PathVariable String movieId) {
        return fluxFlixService.streamTheStreams(movieId);
    }
}

@Service
class FluxFlixService {

    private final MovieRepository movieRepository;

    FluxFlixService(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    public Flux<MovieEvent> streamTheStreams(String movieId) {
        return byId(movieId).flatMapMany(movie -> {
            Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
            Flux<MovieEvent> events = Flux.fromStream(
                    Stream.generate(() -> new MovieEvent(movie, new Date())));
            return Flux.zip(interval, events).map(Tuple2::getT2);
        });

    }

    public Mono<Movie> byId(String movieId) {
        return movieRepository.findById(movieId);
    }

    public Flux<Movie> all() {
        return movieRepository.findAll();
    }
}


@NoArgsConstructor
@AllArgsConstructor
@Data
class MovieEvent {
    private Movie movie;
    private Date when;
}

@NoArgsConstructor
@AllArgsConstructor
@Data
@Document
class Movie {
    private String title;
    @Id
    private String id;
}